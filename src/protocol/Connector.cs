/*
 * Dependencies : 
 * Brunet.AHAddress;
 * Brunet.ConnectionPacket
 * Brunet.ConnectionMessage
 * Brunet.ConnectionMessageParser
 * Brunet.ConnectionType
 * Brunet.ConnectToMessage
 * Brunet.Edge
 * Brunet.IAHPacketHandler
 * Brunet.IPacketSender
 * Brunet.Linker
 * Brunet.Node
 * Brunet.AHPacket
 * Brunet.Packet
 * Brunet.PacketForwarder
 * Brunet.TransportAddress
 */

using System;
using System.Collections;

namespace Brunet
{

  /**
   * sends ConnectToMessage objects out onto the network.
   * This sends the request, and then waits for the response.
   * When it gets the reponse, it creates a linker to link the
   * two nodes.  Once it has completed its job, it sends a FinishEvent.
   *
   * This should *ONLY* be used by ConnectionOverlord subclasses.  This
   * is a very low-level class that has to do with bootstrapping and
   * making sure the nodes have the proper neighbors.
   * 
   * @see CtmRequestHandler
   * @see Linker
   * @see StructuredConnectionOverlord
   * @see UnstructuredConnectionOverlord
   */

  public class Connector:IAHPacketHandler
  {

    /*private static readonly log4net.ILog _log =
        log4net.LogManager.GetLogger(System.Reflection.MethodBase.
        GetCurrentMethod().DeclaringType);*/

    protected Node _local_node;
    /**
     * The node who is making the Connection request
     */
    public Node Node
    {
      get
      {
        return _local_node;
      }
    }

    protected AHPacket _con_packet;
    /**
     * The packet which is being sent out on the network whose
     * payload contains (a potentially PacketForwarder wrapped)
     * ConnectToMessage
     */
    public AHPacket Packet
    {
      get
      {
        return _con_packet;
      }
    }

    protected ArrayList _got_ctms;
    /**
     * Each received CTM is put on this array.  This is
     * so when the finish event is fired, we can see what
     * the received CTMs were
     */
    public ArrayList ReceivedCTMs
    {
      get { return _got_ctms; }
    }

    protected int _req_id;
    /** Holds the id of the response we are looking for */
    public int RequestId
    {
      get
      {
        return _req_id;
      }
    }
    /**
     * This event is fired when this Connector is done working
     */
    public event EventHandler FinishEvent;

    protected int _ctm_send_timeouts;

    protected ConnectionMessageParser _cmp;

    /**
     * All the Linker objects created by this Connector
     * are in this array (in the order created).
     * This is to make sure each Linker does not go out
     * of scope
     */
    protected ArrayList _linkers;
    /**
     * How many time outs are allowed before assuming failure
     */
    protected readonly int MaxTimeOuts = 3;
    /**
     * The AH Network is slower than IP, give it a little
     * while to get to the end node
     */
    protected static readonly int AHMsTimeOut = 5000;
    protected static readonly TimeSpan _timeout;
    protected DateTime _last_packet_datetime;

    /**
     * Either a Node or an Edge to use to send the
     * ConnectToMessage packet
     */
    protected IPacketSender _sender;
    /**
     * Is false until we get a response
     */
    protected bool _got_ctm;
    /**
     * We lock this when we need thread safety
     */
    protected object _sync;
    static Connector() {
      _timeout = new TimeSpan(0, 0, 0, 0, AHMsTimeOut);
    }
    /**
     * @param local the local Node to connect to the remote node
     * @param eh EventHandler to call when we are finished.
     */
    public Connector(Node local)
    {
      _sync = new Object();
      _local_node = local;

      _got_ctms = new ArrayList();
      _linkers = new ArrayList();
      _got_ctm = false;
      _cmp = new ConnectionMessageParser();
    }

    /**
     * We use this Connector to INITIATE connections over the Brunet Network,
     * not to respond to ConnectToMessage requests.
     * @param request_packet the packet to send which already contains a CTM
     * @param req_id the ID of the ConnectToMessage inside request_packet
     */
    public void Connect(AHPacket request_packet, int req_id)
    {
      Connect(_local_node, request_packet, req_id);
    }

    /**
     * We use this Connector to INITIATE connections over the Brunet Network,
     * not to respond to ConnectToMessage requests.
     * @param IPacketSender Use this specific edge.  This is used when we want to
     * connecto to a neighbor of a neighbor
     * @param request_packet the packet to send which already contains a CTM
     * @param req_id the ID of the ConnectToMessage inside request_packet
     */
    public void Connect(IPacketSender ps, AHPacket request_packet, int req_id)
    {
      lock(_sync) {
        _sender = ps;
        ///Listen for response to what we send :
        _local_node.Subscribe(AHPacket.Protocol.Connection, this);
        _req_id = req_id;
        _con_packet = request_packet;
        _ctm_send_timeouts = 0;
        //_log.Info("Sending CTM Request:");
        //_log.Info("CTM Packet:\n" + request_packet.ToString());
        _sender.Send(_con_packet);
        _last_packet_datetime = DateTime.Now;
        _ctm_send_timeouts = 1;
        _local_node.HeartBeatEvent += new EventHandler(this.ResendCtmHandler);
      }
    }

    /**
     * When we listen for responses to our ConnectToMessages,
     * we must implement this method from IAHPacketHandler
     */
    public void HandleAHPacket(object node, AHPacket p, Edge from)
    {
      lock(_sync) {
        try {
          if (p.PayloadType == AHPacket.Protocol.Connection) {
            ConnectionMessage cm = _cmp.Parse(p);
            if ((cm != null) &&
                (cm.Id == _req_id) &&
                (cm.Dir == ConnectionMessage.Direction.Response) &&
                (cm is ConnectToMessage)) {
              /**
              * This is our response.  Now we know who to connect
              * to!
              * @todo see if the type of connection is the same
               */
              ConnectToMessage new_ctm = (ConnectToMessage)cm;
              _got_ctm = true;
              _got_ctms.Add(new_ctm);
              //_log.Info("Got CTM Response: " + cm.ToString());
              Linker l = new Linker(_local_node);
              _linkers.Add(l);
              l.Link(new_ctm.TargetAddress,
                     new_ctm.TransportAddresses,
                     new_ctm.ConnectionType);
            }
          }
        }
        catch(Exception x) {
          //_log.Error(x);
        }
      }
    }

    public bool HandlesAHProtocol(AHPacket.Protocol type)
    {
      return (type == AHPacket.Protocol.Connection);
    }

    /**
     * An event handler that gets called periodically by Node.
     */
    protected void ResendCtmHandler(object node, EventArgs arg)
    {
      bool finish = false;
      try {
        if( DateTime.Now - _last_packet_datetime > _timeout) {
          if( _ctm_send_timeouts >= MaxTimeOuts ) {
            finish = true;
          }
          else if( _got_ctm == false && _ctm_send_timeouts < MaxTimeOuts ) {
            //There has been no response, resend the request
            _sender.Send( _con_packet );
          }
          _last_packet_datetime = DateTime.Now;
          //We have timed out one more time
          _ctm_send_timeouts++;
        }
      }
      catch(Exception x) {
        finish = true;
      }
      finally {
        if( finish ) {
          //We are done now:
          _local_node.HeartBeatEvent -= new EventHandler(this.ResendCtmHandler);
          //Now we have the response :  stop listening
          _local_node.Unsubscribe(AHPacket.Protocol.Connection, this);
          if(FinishEvent != null) {
            FinishEvent(this, null);
          }
        }
      }
    }

  }

}



