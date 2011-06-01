/*
This program is part of BruNet, a library for the creation of efficient overlay networks.
Copyright (C) 2005  University of California
Copyright (C) 2010 P. Oscar Boykin <boykin@pobox.com>  University of Florida

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

#if BRUNET_NUNIT
using NUnit.Framework;
#endif

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using Brunet.Transport;
using Brunet.Concurrent;
using Brunet.Collections;
using BM = Brunet.Messaging;

namespace Brunet.Connections {

  /**
   * These are the Major connection types.  Connections can have subtypes,
   * which are denoted with dots followed by these names:
   * e.g. "structured.near" "structed.shortcut" etc...
   */
  public enum ConnectionType
  {
    Leaf,                       //Connections which are point-to-point edge.
    Structured,                 //Connections for routing structured addresses
    Unstructured,               //Connections for routing unstructured addresses
    Unknown                     //Refers to all connections which are not in the above
  }

  /** Mutable state of a Connection
   */
  public class ConnectionState {
    public readonly Edge Edge;
    public readonly StatusMessage StatusMessage;
    public readonly LinkMessage PeerLinkMessage;
    public readonly bool Disconnected;
    public ConnectionState(Edge e, StatusMessage sm, LinkMessage lm, bool discon) {
      Edge = e;
      PeerLinkMessage = lm;
      StatusMessage = sm;
      Disconnected = discon;
    }
  }

  /**
   * Holds all the data about a connection
   */
#if BRUNET_NUNIT
  [TestFixture]
#endif
  public class Connection{
#if BRUNET_NUNIT
    //NUnit needs a default constructor
    public Connection() { }
#endif

    /**
     * Prefered constructor for a Connection
     */
    public Connection(Edge e, Address a,
		      string connectiontype,
		      StatusMessage sm, LinkMessage peerlm)
    {
      if( null == a ) {
        throw new System.ArgumentNullException(
                    String.Format("Address cannot be null in Connection: Edge: {0} contype: {1}", e, connectiontype));
      }
      Address = a;
      ConType = String.Intern(connectiontype);
      CreationTime = DateTime.UtcNow;
      MainType = StringToMainType(ConType);
      //Mutable state:
      var cs = new ConnectionState(e, sm, peerlm, false);
      _state = new Mutable<ConnectionState>(cs);
    }

    public readonly DateTime CreationTime;
    public readonly Address Address;
    public readonly ConnectionType MainType;
    public readonly string ConType;

    public event Action<Connection, Pair<ConnectionState, ConnectionState>> StateChangeEvent;

    protected readonly Mutable<ConnectionState> _state;   
    public ConnectionState State { get { return _state.State; } }

    public string SubType {
      get {
        string res;
        int dot_idx = ConType.IndexOf('.');
        if( dot_idx >= 0 ) {
          res = ConType.Substring(dot_idx);
        }
        else {
          res = String.Empty;
        }
        return res;
      }
    }

// ///////////////
// Methods
// ///////////////
    /*** Immediately close the underlying edge, and don't warn the other node.
     * Prefer Connection.Close if possible.  This is idempotent.
     * @return the old state, new state pair.
     */
    public Pair<ConnectionState,ConnectionState> Abort() {
      var res = _state.Update(delegate(ConnectionState old_state) {
        if( old_state.Disconnected ) { return old_state; }
        else {
          return new ConnectionState(old_state.Edge,
                                     old_state.StatusMessage,
                                     old_state.PeerLinkMessage, true);
        }
      });
      if( res.First != res.Second ) {
        //Only send the event if there is an actual change
        var ev = StateChangeEvent;
        if( null != ev ) {
          ev(this, res);
        }
      }
      return res;
    }
    /*** Gracefully close this connection, if it is not already disconnected.
     * Idempotent (calling it twice is the same as once).
     * @return the old state, new state pair.
     */
    public Pair<ConnectionState,ConnectionState> Close(BM.RpcManager rpc, string reason) {
      var old_new = Abort();
      if( old_new.First.Disconnected != true ) {
        //Now try to tell the other node:
        var close_info = new ListDictionary(); 
        if( reason != String.Empty ) {
          close_info["reason"] = reason;
        }
        Edge e = old_new.Second.Edge;
        var results = new Channel(1);
        //Either the RPC call times out, or we get a response.
        results.CloseEvent += delegate(object o, EventArgs args) {
          e.Close();
        };
        try { rpc.Invoke(e, results, "sys:link.Close", close_info); }
        catch { e.Close(); }
      }
      return old_new;
    }

    /** return the old state, and new state
     */
    public Pair<ConnectionState,ConnectionState> SetEdge(Edge e, LinkMessage lm) {
      var res = _state.Update(delegate(ConnectionState old_state) {
        if( old_state.Disconnected ) {
          throw new Exception(String.Format("Connection: {0} is disconnected",this));
        }
        var new_state = new ConnectionState(e, old_state.StatusMessage, lm, false);
        return new_state;
      });
      var ev = StateChangeEvent;
      if( null != ev ) {
        ev(this, res);
      }
      return res;
    }
    public Pair<ConnectionState,ConnectionState> SetState(ConnectionState cs) {
      var res = _state.Update(delegate(ConnectionState old_state) {
        if( old_state.Disconnected ) {
          throw new Exception(String.Format("Connection: {0} is disconnected",this));
        }
        return cs;
      });
      if( res.First != res.Second ) {
        //Only send the event if there is an actual change
        var ev = StateChangeEvent;
        if( null != ev ) {
          ev(this, res);
        }
      }
      return res;
    }
    /** return the old state, and new state
     */
    public Pair<ConnectionState,ConnectionState> SetStatus(StatusMessage sm) {
      var res = _state.Update(delegate(ConnectionState old_state) {
        if( old_state.Disconnected ) {
          throw new Exception(String.Format("Connection: {0} is disconnected",this));
        }
        var new_state = new ConnectionState(old_state.Edge, sm, old_state.PeerLinkMessage, false);
        return new_state;
      });
      var ev = StateChangeEvent;
      if( null != ev ) {
        ev(this, res);
      }
      return res;
    }

    /**
     * Return the string for a connection type
     */
    static public string ConnectionTypeToString(ConnectionType t)
    {
      if( t == ConnectionType.Structured ) {
        return "structured";
      }
      if( t == ConnectionType.Leaf ) {
        return "leaf";
      }
      return String.Intern( t.ToString().ToLower() );
    }

    /**
     * Doing string operations is not cheap, and we do this a lot
     * so it is worth improving the performance
     */
    static protected Dictionary<string, ConnectionType> _string_to_main_type 
        = new Dictionary<string, ConnectionType>();
    /**
     * Return the string representation of a ConnectionType
     */
    static public ConnectionType StringToMainType(string s)
    {
      ConnectionType result;
      lock( _string_to_main_type ) {
        if( false ==_string_to_main_type.TryGetValue(s, out result)) {
          int dot_idx = s.IndexOf('.');
          string maintype = s;
          if( dot_idx > 0 ) {
            maintype = s.Substring(0, dot_idx);
          }
          try {
            result = (ConnectionType)Enum.Parse(typeof(ConnectionType),
                                               maintype,
                                               true);
          }
          catch { result = ConnectionType.Unknown; }
          _string_to_main_type[String.Intern(s)] = result;
        }
      }
      return result;
    }

    /** Return a version of the Dictionary suitable for ADR use
     * maps:
     * "address" => Address.ToString()
     * "sender" => Edge.ToUri()
     * "type" => ConType
     */

    public IDictionary ToDictionary() {
      ListDictionary ld = new ListDictionary();
      ld.Add("address", Address.ToString());
      ld.Add("sender", _state.State.Edge.ToUri());
      ld.Add("type", ConType);
      return ld;
    }
    //Keys used in the ToDictionary method
    public static readonly string[] DictKeys = new string[]{"address", "sender", "type"};

    /**
     * @return a string representation of the Connection
     */
    public override string ToString()
    {
      return String.Format("Edge: {0}, Address: {1}, ConnectionType: {2}",
                                    State.Edge, Address, ConType);
    }
    public string ToUri() {
      throw new NotImplementedException("Connection.ToUri() not implement");
    }
#if BRUNET_NUNIT
    [Test]
    public void TestParsing() {
      Assert.AreEqual(ConnectionType.Structured, StringToMainType("structured.near"));
      Assert.AreEqual(ConnectionType.Structured, StringToMainType("structured.shortcut"));
      Assert.AreEqual(ConnectionType.Structured, StringToMainType("structured"));
      Assert.AreEqual(ConnectionType.Unstructured, StringToMainType("unstructured"));
      Assert.AreEqual(ConnectionType.Unknown, StringToMainType("asdasfba"));
    }
#endif
  }
	  
}
