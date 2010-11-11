/*
Copyright (C) 2008  David Wolinsky <davidiw@ufl.edu>, University of Florida

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
FITNESS FOR A PARTICULAR PURP[OSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

using Brunet;
using Brunet.Util;
using Brunet.Transport;
using Brunet.Security;
using Brunet.Security.Transport;
using System;
using System.Collections;
using System.Threading;

using Brunet.Messaging;
using Brunet.Symphony;
namespace Brunet.Applications {
  /**
  <summary>This class provides a Rpc to be used during crawls of the network.
  It provides base informatoin such as geographical coordinates, local end
  points, the type of node, and neighbors.  There is also support for user's
  to put their own data in the return value.</summary>
  */
  public class Information: IRpcHandler {
    /// <summary>Only want one thread of geo lookup at a time</summary>
    protected bool in_geoloc = false;
    /// <summary>Last time the geographical update service was called</summary>
    protected DateTime _last_called = DateTime.UtcNow - TimeSpan.FromHours(48);
    /// <summary>The rpc service provider.</summary>
    protected RpcManager _rpc;
    /// <summary>The geographical coords for the system.</summary>
    protected String geo_loc = ",";
    /// <summary>A hashtable providing user information.</summary>
    public Hashtable UserData;
    /// <summary>The node where service is being provided.</summary>
    protected readonly StructuredNode _node;
    /// <summary>The name of the application providing service.</summary>
    protected readonly String _type;
    public SecurityOverlord _so;


    /**
    <summary>Creates an Information object for the node and type of service
    provider.</summary>
    <param name="node">The node where the service is to be provided</param>
    <param name="type">The name of the application providing service (example:
    BasicNode)</param>
    */
    public Information(StructuredNode node, String type) {
      UserData = new Hashtable();
      _type = type;
      _node = node;
      _rpc = node.Rpc;
      _rpc.AddHandler("Information", this);
    }

    public Information(StructuredNode node, String type, SecurityOverlord so) :
      this(node, type)
    {
      _so = so;
    }

    /**
    <summary>Implements IRpcHandler that is called by the RpcManager when an
    Rpc method requests any method with Information.*.</summary>
    <param name="caller">The remote caller</param>
    <param name="method">The name of the method called.</param>
    <param name="arguments">An IList of arguments supplied</param>
    <param name="request_state">Provides a return path back to the caller.
    </param>
    */
    public void HandleRpc(ISender caller, String method, IList arguments,
                          object request_state) {
      object result = new InvalidOperationException("Invalid method");
      if(method.Equals("Info")) {
        result = this.Info();
      }
      _rpc.SendResult(request_state, result);
    }

    /**
    <summary>This returns all the gathered information as a hashtable.
    Particularly useful for crawling.</summary>
    <returns>The dictionary (hashtable) containing the information about the
    node.</returns>
    */
    public IDictionary Info() {
      GetGeoLoc();
      Hashtable ht = new Hashtable(UserData);
      ht.Add("type", _type);
      ht.Add("geo_loc", geo_loc);
      ht.Add("localips", _node.sys_link.GetLocalIPAddresses());
      ht.Add("neighbors", _node.sys_link.GetNeighbors());
      int wedge_count = 0;

      foreach(EdgeListener el in _node.EdgeListenerList) {
        WrapperEdgeListener wel = el as WrapperEdgeListener;
        if(wel != null) {
          try {
            wedge_count += el.Count;
            ht.Add(TransportAddress.TATypeToString(wel.TAType), wel.UnderlyingCount);
          } catch { }
        } else {
          int count = 0;
          try {
            count = el.Count;
          } catch { }
          ht.Add(TransportAddress.TATypeToString(el.TAType), count);
        }
      }

      ht.Add("cons", _node.ConnectionTable.TotalCount);
      if(wedge_count != 0) {
        ht.Add("wedges", wedge_count);
      }
      if(_so != null) {
        ht.Add("sas", _so.SACount);
      }
      return ht;
    }

    /**
    <summary>Used to have Geographical coordinates looked up in another thread.
    </summary>
    */
    protected void GetGeoLoc() {
      DateTime now = DateTime.UtcNow;
      if((now - _last_called > TimeSpan.FromDays(7) || geo_loc.Equals(","))) {
        lock(geo_loc) {
          if(in_geoloc) {
            return;
          }
          in_geoloc = true;
        }
        ThreadPool.QueueUserWorkItem(GetGeoLocAsThread, null);
      }
    }

    /// <summary>Sets the geographical location of the running node.</summary>
    protected void GetGeoLocAsThread(object o) {
      String local_geo_loc = Utils.GetMyGeoLoc();
      if(!local_geo_loc.Equals(",")) {
        geo_loc = local_geo_loc;
      }
      in_geoloc = false;
    }
  }
}
