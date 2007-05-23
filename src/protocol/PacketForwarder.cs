/*
This program is part of BruNet, a library for the creation of efficient overlay
networks.
Copyright (C) 2005  University of California
Copyright (C) 2007 P. Oscar Boykin <boykin@pobox.com>, University of Florida

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

namespace Brunet
{

  /**
   * Implements the Packet Forwarding protocol which is
   * used to Bootstrap new connections on the network (and
   * potentially for other uses in the future).
   *
   * The Basic idea is to send a packet from A->B->C preserving
   * the information so that C can do: C->B->A (a path that should
   * exist).
   */
  public class PacketForwarder : IDataHandler
  {

    /*private static readonly log4net.ILog _log =
        log4net.LogManager.GetLogger(System.Reflection.MethodBase.
        GetCurrentMethod().DeclaringType);*/
    
    protected Address _local;
    protected Node _n;

    public PacketForwarder(Node local)
    {
      _n = local;
      _local = _n.Address;
    }

    /**
     * This handles the packet forwarding protocol
     */
    public void HandleData(MemBlock b, ISender ret_path, object state)
    {
      /*
       * Check it
       */
      AHSender ahs = ret_path as AHSender;
      if( ahs != null ) {
        //This was an AHSender:
        /*
         * This goes A -> B -> C
         */
        if( b[0] == 0 ) {
          //This is the first leg, going from A->B
          Address add_c = AddressParser.Parse(b.Slice(1, Address.MemSize));
          //Since ahs a sender to return, we would be the source:
          Address add_a = ahs.Destination;
          MemBlock payload = b.Slice( 1 + Address.MemSize );
          MemBlock f_header = MemBlock.Reference( new byte[]{1} );
          /*
           * switch the packet from [A B f0 C] to [B C f 1 A]
           */
          ICopyable new_payload = new CopyList(PType.Protocol.Forwarding,
                                           f_header, add_a, payload);
          AHSender next = new AHSender(_n, ahs.ReceivedFrom, add_c,
                                       _n.DefaultTTLFor(add_c),
                                       AHPacket.AHOptions.AddClassDefault); 
          next.Send(new_payload);
        }
        else if ( b[0] == 1 ) {
          /*
           * This is the second leg: B->C
           * Make a Forwarding Sender, and unwrap the inside packet
           */
          Address add_a = AddressParser.Parse(b.Slice(1, Address.MemSize));
          Address add_b = ahs.Destination;
          MemBlock rest_of_payload = b.Slice(1 + Address.MemSize);
          //Here's the return path:
          ISender new_ret_path = new ForwardingSender(_n, add_b, add_a);
          _n.Announce(rest_of_payload, new_ret_path);
        }
      }
      else {
        //This is not (currently) supported.
        System.Console.Error.WriteLine("Got a forwarding request from: {0}", ret_path);
      }
    }
  }

  /**
   * This is an ISender which forwards a packet through another node
   */
  public class ForwardingSender : ISender {
    protected ISender _sender;
    protected ICopyable _header;
    
    protected Address _dest;
    public Address Destination { get { return _dest; } }

    public ForwardingSender(Node n, Address forwarder, Address destination) {
      _dest = destination;
      _sender = new AHSender(n, forwarder);
      _header = new CopyList(PType.Protocol.Forwarding,
                             MemBlock.Reference(new byte[]{0}),
                             destination);
    }

    /* 
     * Send a packet by forwarding it first.
     */
    public void Send(ICopyable d) {
      _sender.Send( new CopyList(_header, d) );
    }

    override public int GetHashCode() {
      return _dest.GetHashCode();
    }
    override public bool Equals(object o) {
      ForwardingSender fs = o as ForwardingSender;
      bool eq = false;
      if( fs != null ) {
        eq = fs.Destination.Equals( _dest ); 
      }
      return eq;
    }
    override public string ToString() {
      return System.String.Format("ForwardingSender({0} -> {1})",_sender, _dest);
    }
  }
}
