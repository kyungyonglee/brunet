using System;
using System.Collections;

using Brunet.Util;
using Brunet.Messaging;
using Brunet.Concurrent;
using Brunet.Symphony;
//using System.Collections.Specialized;

namespace Brunet.Services.MapReduce {
  public class MrTaskAutoRunner{
    private SimpleTimer _timer;
    public SimpleTimer Timer { 
      get {return _timer;}
      set {_timer = value;}
    }
    public readonly string TaskName;
    public readonly Node LocalNode;
    private int _send_rate;  //default = 1/100
    public int SendRate { 
      get {return _send_rate;}
      set {_send_rate = value;}
    }
    private int _check_period;  //check every 30 seconds
    public int CheckPeriod { 
      get {return _check_period;}
      set {_check_period = value;}
    }
    private string _end_addr;
    public string EndAddr { 
      get {return _end_addr;}
      set {_end_addr = value;}
    }

    public MrTaskAutoRunner(string task_name, Node node){
      TaskName = task_name;
      LocalNode = node;
      Init(100, 30000);
    }
    
    public MrTaskAutoRunner(string task_name, Node node, int send_rate, int check_period){
      TaskName = task_name;
      LocalNode = node;
      Init(send_rate, check_period);
    }

    private void Init(int send_rate, int check_period){
      _send_rate = send_rate;
      _check_period = check_period;
      _end_addr = GetEndAddr(LocalNode);
      _timer = new SimpleTimer(CheckSend, null, _check_period, _check_period);
      _timer.Start();
    }
    private  void CheckSend(object o){
      System.Random r = new Random((int)DateTime.UtcNow.Ticks);
      int rn = r.Next(1,_send_rate+1);
      Console.WriteLine("rn = " + rn);
      if(rn == _send_rate){
        SendMrTask();
      }
    }
    
    private void SendMrTask(){
      Console.WriteLine("Send mr task task_name is " + TaskName + "end_addr is " + _end_addr);
      Channel ret_queue = new Channel(1);
      Hashtable ht = new Hashtable();
      ht.Add("task_name", TaskName);
      ht.Add("gen_arg", _end_addr);
      ret_queue.CloseEvent += MrResultHanlder;
      LocalNode.Rpc.Invoke(LocalNode, ret_queue, "mapreduce.Start", ht);
    }

    private void MrResultHanlder(object q, EventArgs args){
      Channel ret = (Channel)q;
      double consistency = 0.0;
      try {
        RpcResult mr_rpc_result = ret.Dequeue() as RpcResult;
        Hashtable result = mr_rpc_result.Result as Hashtable;
        consistency = (double)((double)result["consistency"] / (int)result["count"]);
      }
      catch(Exception x) {Console.WriteLine(x);return;}

      Console.WriteLine("consistency = " + consistency);
      // Add a module to save the consistency value into the DHT
    }

    private string GetEndAddr(Node node){
      BigInteger bi_addr = node.Address.ToBigInteger();
      bi_addr -= 2;
      Address end_addr = new AHAddress(bi_addr);
      return end_addr.ToString();      
    }
  }
}

