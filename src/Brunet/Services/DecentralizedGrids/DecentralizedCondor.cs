using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.IO;

using Brunet.Util;
using Brunet.Services.XmlRpc;
using Brunet.Services.Dht;
using Brunet.Messaging;
using Brunet.Concurrent;
using Brunet.Symphony;

using CondorSoap;

namespace Brunet.Services.DecenGrids{
  public enum AGGREGATION_MODE{
    AGGREGATE_WITH_FULL_INFORMATION,
    NO_AGGREGATION,
    ROOT_NODE_CONTACTS_EACH_NODE,
    CHANGE_CONDOR_HOST
  }

  public enum CondorDataType : byte{
    ClassAdsRequest = 1, 
    ClassAdsUpdate = 2, 
    RemoveClassAds = 3, 
    ChangeCondorHost = 4, 
    CondorHostToOriginal = 5, 
    ChangeHostCmdAck = 6
  }

  public class DecentralizedCondor{
    protected int _schedd_polling_period_msec;
    protected Node _node;
//    private SimpleTimer _timer;
    protected CondorSoapInterface _csi; 
    protected ProcessObject _po;
    public readonly string PartialAddress;
    protected AGGREGATION_MODE _aggregation_method;
    protected CondorDataHandler _cdh;
    protected long _last_callback_ticks;
    protected object _callback_tick_lock;
    protected string _condor_host;
     
    public DecentralizedCondor(int schedd_polling_period_msec, Node node){
      _schedd_polling_period_msec = schedd_polling_period_msec;
      _node = node;
      PartialAddress = node.Address.ToString().Substring(12);  //12 = brunet:node:
      _aggregation_method = AGGREGATION_MODE.CHANGE_CONDOR_HOST;
      _po = new ProcessObject();

      _po.CondorConfigVal.StartInfo.Arguments = "-startd -rset \"BRUNET_ADDRESS = \\\"" + PartialAddress + "\\\"\"";
      Console.WriteLine("-startd -rset \"BRUNET_ADDRESS = \\\"" + PartialAddress + "\\\"\"");
      _po.CondorConfigVal.Start();
      _po.CondorConfigVal.WaitForExit();
      _po.CondorConfigVal.StartInfo.Arguments = "-startd -rset \"STARTD_ATTRS = \\$(STARTD_ATTRS) COORDINATE LATITUDE LONGITUDE BRUNET_ADDRESS\"";
      Console.WriteLine("-startd -rset \"STARTD_ATTRS = \\$(STARTD_ATTRS) BRUNET_ADDRESS\"");
      _po.CondorConfigVal.Start();
      _po.CondorConfigVal.WaitForExit();
      _po.CondorReconfig.Start();
      _po.CondorReconfig.WaitForExit();

      _csi = new CondorSoapInterface();
//      _csi.InsertCoordinate(_node);

      _callback_tick_lock = new object();

      _po.CondorConfigVal.StartInfo.Arguments = "local_dir";
      _po.CondorConfigVal.StartInfo.RedirectStandardOutput = true;
      _po.CondorConfigVal.StartInfo.UseShellExecute = false;
      _po.CondorConfigVal.Start();
      string local_dir = _po.CondorConfigVal.StandardOutput.ReadToEnd();
      _po.CondorConfigVal.WaitForExit();
      
      _po.CondorConfigVal.StartInfo.Arguments = "CONDOR_HOST";
      _po.CondorConfigVal.Start();
      _condor_host = _po.CondorConfigVal.StandardOutput.ReadToEnd();
      _condor_host = _condor_host.Trim(new char[] {'\n'});
      Console.WriteLine("_condor_host = {0}", _condor_host);
      _po.CondorConfigVal.WaitForExit();

      _cdh = new CondorDataHandler(_node, _csi, _condor_host);
      
      _po.CondorConfigVal.StartInfo.RedirectStandardOutput = false;
      _po.CondorConfigVal.StartInfo.UseShellExecute = true;
      
      char[] trim_char = {'\n'};
      local_dir = local_dir.Trim(trim_char);
      Console.WriteLine("local_dir = {0}", local_dir);
      FileWatcher(local_dir+"/log", "SchedLog");
      lock(_callback_tick_lock){
        _last_callback_ticks = DateTime.UtcNow.Ticks;
      }
      new CondorControlAction(_node);
//      _timer = new SimpleTimer(Callback, null, _schedd_polling_period_msec, 0);
//      _timer.Start();
    }

    private void Callback(object source, FileSystemEventArgs e){
      Console.WriteLine("Decentralized Debug: Callback called");
      lock(_callback_tick_lock){
        if((DateTime.UtcNow.Ticks - _last_callback_ticks) < (TimeSpan.TicksPerSecond*60)){
          return;
        }else{
          _last_callback_ticks = DateTime.UtcNow.Ticks;
        }
      }
      
      Hashtable mr_arg = null;
      Channel ret = new Channel(1);
      bool new_ad_inserted = false;
//      int callback_index = 0;
      ClassAdStructAttr[][] job_casa_lists = _csi.GetJobAds("JobStatus == 1"); //JobStatus Enum: 0 Unexpanded  U, 1 Idle  I,2 Running R, 3 Removed X, 4 Completed C, 5 Held  H, 6 Submission_err  E
      if(job_casa_lists.Length == 0){
        RemoveOtherStartd();
        return;
      }
      List<List<ClassAdStructAttr[]>> jobads_lists = OrganizeJobAds(job_casa_lists);
      foreach(List<ClassAdStructAttr[]> casa_lists in jobads_lists){
        Console.WriteLine("Decentralized Debug: Before MapReduce RPC");
        mr_arg = CreateMapReduceArgs(casa_lists.Count, casa_lists[0]);
   //     Interlocked.Increment(ref callback_index);
        ret.CloseEvent += delegate(object oo, EventArgs args) {
          Console.WriteLine("Decentralized Debug: CloseEvent Called");
          Channel queue = (Channel) oo;
          Console.WriteLine("Decentralized Debug: after cnvert to Channel");
          RpcResult rs = queue.Dequeue() as RpcResult;
          Console.WriteLine("Decentralized Debug: after dequeue");
          Hashtable discovery_result = null;
          try{
            discovery_result = rs.Result as Hashtable;
          }catch(Exception ee){Console.WriteLine(ee);}
          Console.WriteLine("Decentralized Debug: after hashtable conversion");
        
          if(discovery_result != null){
            switch(_aggregation_method){
              case AGGREGATION_MODE.AGGREGATE_WITH_FULL_INFORMATION:
                foreach(DictionaryEntry de in discovery_result){
                  ArrayList classad_info = de.Value as ArrayList;
                  if(classad_info != null){
                    _csi.insertAd(CondorSoap.ClassAdType.STARTDADTYPE, CondorSoapInterface.ConvertToCasa(classad_info));
                    new_ad_inserted = true;
                  }else{
                    Console.WriteLine("Not a ClassAd: {0} = {1}", de.Key as string, (int)de.Value);
                  }
                }
                break;
              case AGGREGATION_MODE.NO_AGGREGATION:
                break;
              case AGGREGATION_MODE.CHANGE_CONDOR_HOST:
                foreach(DictionaryEntry de in discovery_result){
                  string unicast_addr = de.Key as string;
                  if(unicast_addr != null && unicast_addr.Contains("brunet:node:")){
                    Address addr = AddressParser.Parse(unicast_addr);
                    MemoryStream ms = new MemoryStream();
                    int serialized = AdrConverter.Serialize(_condor_host, ms);
                    byte[] sbuf = new byte[serialized + 1];
                    int s2count = AdrConverter.Serialize(_condor_host, sbuf, 1);
                    sbuf[0] = (byte)CondorDataType.ChangeCondorHost;
                    _cdh.SendMessage(addr, MemBlock.Reference(sbuf));
                  }
                }                
                break;
              case AGGREGATION_MODE.ROOT_NODE_CONTACTS_EACH_NODE:
              default:
                foreach(DictionaryEntry de in discovery_result){
                  string unicast_addr = de.Key as string;
                  if(unicast_addr != null && unicast_addr.Contains("brunet:node:")){
                    Address addr = AddressParser.Parse(unicast_addr);
                    byte[] byte_str = new byte[1];
                    byte_str[0] = (byte)CondorDataType.ClassAdsRequest;
                    _cdh.SendMessage(addr, MemBlock.Reference(byte_str));
                  }
                }
                break;
            }
          }
          
          if(new_ad_inserted == true){
            _po.CondorReschedule.Start();
            new_ad_inserted = false;
            Console.WriteLine("Decentralized Debug: reschedule called");
          }
 //         Interlocked.Decrement(ref callback_index);
 //         if(callback_index == 0){
 //           _timer.Stop();
 //           Console.WriteLine("Decentralized Debug: Start New Timer");
 //           _timer = new SimpleTimer(Callback, null, _schedd_polling_period_msec, 0);
 //           _timer.Start();
 //         }
        };
        _node.Rpc.Invoke(_node, ret, "mapreduce.Start", mr_arg);
      }
      
 //     if(callback_index == 0){
 //       _timer.Stop();
 //       Console.WriteLine("Decentralized Debug: Start New Timer");
 //       _timer = new SimpleTimer(Callback, null, _schedd_polling_period_msec, 0);
 //       _timer.Start();
 //     }

    }

    private List<List<ClassAdStructAttr[]>> OrganizeJobAds(ClassAdStructAttr[][] casa_lists){
      int i;
      bool found = false;
      Console.WriteLine("Decentralized Debug: number of jobads is {0}", casa_lists.Length);
      var job_lists = new List<List<ClassAdStructAttr[]>>();
      foreach(ClassAdStructAttr[] casa_job in casa_lists ){
        found = false;
        if(job_lists.Count == 0){
          job_lists.Add(new List<ClassAdStructAttr[]>());
          job_lists[0].Add(casa_job);
          continue;
        }
        foreach(ClassAdStructAttr element in casa_job){
          if(String.Compare(element.name, "Requirements", true) == 0){
            for(i=0;i<job_lists.Count;i++){
              if(job_lists[i].Count > 0){
                foreach(ClassAdStructAttr entry in job_lists[i][0]){  //check only the first job list
                  if(String.Compare(entry.name, "Requirements", true) == 0){
                    if(String.Compare(entry.value, element.value, true) == 0){
                      job_lists[i].Add(casa_job);
                      found = true;
                    }else{
                      job_lists.Add(new List<ClassAdStructAttr[]>());
                      job_lists[(job_lists.Count-1)].Add(casa_job);
                      found = true;
                    }
                    break;
                  }
                }
              }
              if(found == true){
                break;
              }
            }
            break;    //break : foreach(ClassAdStructAttr element in casa_job)
          }
        }
      }
      Console.WriteLine("Decentralized Debug: number of req = {0}", job_lists.Count);
      for(int k=0;k<job_lists.Count;k++){
        Console.WriteLine("Decentralized Debug: each entry's job number is {0}", job_lists[k].Count);
      }
      return job_lists;
    }

    public Hashtable CreateMapReduceArgs(int num_res, ClassAdStructAttr[] classads){
      Hashtable mr_arg = new Hashtable();
      Hashtable red_arg = new Hashtable();
      Hashtable map_arg = new Hashtable();
      
      Console.WriteLine("Decentralized Debug: CreateMapReduceArgs : num_Res = {0}", num_res);
      switch(_aggregation_method){
        case AGGREGATION_MODE.AGGREGATE_WITH_FULL_INFORMATION:
          mr_arg.Add("task_name", "Brunet.Services.MapReduce.MapReduceMatchmaking");
          mr_arg.Add("map_arg", CondorSoapInterface.ConvertToClassAdStyle(classads));
          break;
        case AGGREGATION_MODE.NO_AGGREGATION:
          mr_arg.Add("task_name", "Brunet.Services.MapReduce.MapReduceNoAggrMM");
          map_arg.Add("root_node", _node.Address.ToString());
          map_arg.Add("job_ads", CondorSoapInterface.ConvertToClassAdStyle(classads));
          mr_arg.Add("map_arg", map_arg);
          break;
        case AGGREGATION_MODE.ROOT_NODE_CONTACTS_EACH_NODE:
        case AGGREGATION_MODE.CHANGE_CONDOR_HOST:
        default:
          mr_arg.Add("task_name", "Brunet.Services.MapReduce.MapReduceRootUnicastMM");
          mr_arg.Add("map_arg", CondorSoapInterface.ConvertToClassAdStyle(classads));
          break;
      }
      
      red_arg.Add("num_res", num_res);
      red_arg.Add("first_fit", 1);
      mr_arg.Add("gen_arg", DetermineQueryRange());
      mr_arg.Add("wait_factor", 4);
      mr_arg.Add("reduce_arg", red_arg);
      return mr_arg;
    }

    public static ArrayList DetermineQueryRange(){
      ArrayList al = new ArrayList();
      al.Add("brunet:node:Q56AL3R3G3FALZKYQLGSB3OMTFC2IARG");
      al.Add("brunet:node:Q56AL3R3G3FALZKYQLGSB3OMTFC2IARE");
      return al;
    }

    public void FileWatcher(string path, string file_name){
      FileSystemWatcher watcher = new FileSystemWatcher();
      Console.WriteLine("{0} {1}", path, file_name);
      watcher.Path = path;
      watcher.Filter = file_name;
      watcher.NotifyFilter = NotifyFilters.LastWrite;
      watcher.Changed += new FileSystemEventHandler(Callback);
      watcher.EnableRaisingEvents = true;
    }
/*
    public void SendRollbackHostCmd(ArrayList no_remove_host){
      lock(_condor_host_al_lock){
        foreach(string addr in _condor_host_candidate){
          if(no_remove_host.Contains(addr.Substring(12)) == false){
            Console.WriteLine("Decentralzed Debug: sent roll back command to {0}", addr);
            MemoryStream ms = new MemoryStream();
            int serialized = AdrConverter.Serialize(_condor_host, ms);
            byte[] sbuf = new byte[serialized + 1];
            int s2count = AdrConverter.Serialize(_condor_host, sbuf, 1);
            sbuf[0] = (byte)CondorDataType.CondorHostToOriginal;
            _cdh.SendMessage(AddressParser.Parse(addr), MemBlock.Reference(sbuf));
            _condor_host_candidate.Remove(addr);
          }
        }
        
        foreach(string addr in no_remove_host){
          if(_condor_host_candidate.Contains("brunet:node:"+addr) == false){
            _condor_host_candidate.Add("brunet:node:"+addr);
            Console.WriteLine("Decentralzed Debug: add {0} to candidate hosts", addr);            
          }
        }
      }
    }
    
    protected void RemoveOtherStartd(){   //called when all jobs are done
      ArrayList no_remove_host = new ArrayList();
      if(File.Exists("not"+_node.Address.ToString().Substring(12)) == false){
        using(StreamWriter  sw = new StreamWriter("not"+_node.Address.ToString().Substring(12))){
          sw.Write("MyType = \"Query\"\nTargetType = \"Machine\"\nRequirements = (BRUNET_ADDRESS != \"" + _node.Address.ToString().Substring(12) + "\") && State != \"Claimed\"");
        }
      }
      _po.CondorAdvertise.StartInfo.Arguments = "INVALIDATE_STARTD_ADS not" + _node.Address.ToString().Substring(12);
      _po.CondorAdvertise.Start();
      _po.CondorAdvertise.WaitForExit();
      ClassAdStructAttr[][] casa_array = _csi.GetStartdAds("Requirements = BRUNET_ADDRESS != \"" + _node.Address.ToString().Substring(12) + "\"");
      foreach(ClassAdStructAttr[] one_casa in casa_array){
        foreach(ClassAdStructAttr casa_entry in one_casa){
          if(casa_entry.name.Equals("BRUNET_ADDRESS")){
            no_remove_host.Add(casa_entry.value);
            Console.WriteLine("Decentralized Debug: no_remove_host_ addr = {0}", casa_entry.value);
          }
        }
      }
      
      SendRollbackHostCmd(no_remove_host);
    }
*/
    public void SendRollbackHostCmd(ArrayList remove_host){
      foreach(string addr in remove_host){
        Console.WriteLine("Decentralzed Debug: sent roll back command to {0}", addr);
        MemoryStream ms = new MemoryStream();
        int serialized = AdrConverter.Serialize(_condor_host, ms);
        byte[] sbuf = new byte[serialized + 1];
        int s2count = AdrConverter.Serialize(_condor_host, sbuf, 1);
        sbuf[0] = (byte)CondorDataType.CondorHostToOriginal;
        _cdh.SendMessage(AddressParser.Parse(addr), MemBlock.Reference(sbuf));
      }
    }
    
    protected void RemoveOtherStartd(){   //called when all jobs are done
      ArrayList remove_host_list = new ArrayList();
      ArrayList no_remove_host_list = new ArrayList();
      ClassAdStructAttr[][] not_claimed_casa_array = _csi.GetStartdAds("BRUNET_ADDRESS != \"" + _node.Address.ToString().Substring(12) + "\" && State != \"Claimed\"");
      ClassAdStructAttr[][] claimed_casa_array = _csi.GetStartdAds("BRUNET_ADDRESS != \"" + _node.Address.ToString().Substring(12) + "\" && State == \"Claimed\"");
      foreach(ClassAdStructAttr[] one_casa in not_claimed_casa_array){
        foreach(ClassAdStructAttr casa_entry in one_casa){
          if(String.Compare(casa_entry.name, "BRUNET_ADDRESS", true) == 0){
            if(remove_host_list.Contains("brunet:node:"+casa_entry.value) == false){
              remove_host_list.Add("brunet:node:"+casa_entry.value);
              Console.WriteLine("Decentralized Debug: remove_host_ addr = {0}", casa_entry.value);
            }
          }
        }
      }

      foreach(ClassAdStructAttr[] one_casa in claimed_casa_array){
        foreach(ClassAdStructAttr casa_entry in one_casa){
          if(String.Compare(casa_entry.name, "BRUNET_ADDRESS", true) == 0){
            if(no_remove_host_list.Contains("brunet:node:"+casa_entry.value) == false){
              no_remove_host_list.Add("brunet:node:"+casa_entry.value);
              Console.WriteLine("Decentralized Debug: no_remove_host_ addr = {0}", casa_entry.value);
            }
          }
        }
      }

      foreach(string s in no_remove_host_list){
        if(remove_host_list.Contains(s)){
          remove_host_list.Remove(s);
        }
      }
      SendRollbackHostCmd(remove_host_list);
      if(File.Exists("not"+_node.Address.ToString().Substring(12)) == false){
        using(StreamWriter  sw = new StreamWriter("not"+_node.Address.ToString().Substring(12))){
          sw.Write("MyType = \"Query\"\nTargetType = \"Machine\"\nRequirements = (BRUNET_ADDRESS != \"" + _node.Address.ToString().Substring(12) + "\") && State != \"Claimed\"");
        }
      }
      _po.CondorAdvertise.StartInfo.Arguments = "INVALIDATE_STARTD_ADS not" + _node.Address.ToString().Substring(12);
      _po.CondorAdvertise.Start();
    }
  }

  public class CondorDataHandler : IDataHandler {
    public readonly PType CDH;
    protected Node _node;
    protected CondorSoapInterface _csi;
    protected ProcessObject _po;
    protected string _condor_host;
    
    public CondorDataHandler(Node node, CondorSoapInterface csi, string condor_host){
      CDH = new PType("CondorDataHandler");
      _node = node;
      _csi = csi;
      _condor_host = condor_host;
      _po = new ProcessObject();
      _node.DemuxHandler.GetTypeSource(CDH).Subscribe(this, null);
    }

    public void HandleData(MemBlock payload, ISender return_path, object state) {
      CondorDataType type = (CondorDataType)payload[0];
      MemBlock rest = null;
      rest = payload.Slice(1);
      ArrayList classad_al;
      Console.WriteLine("Decentralized Debug: CondorDataHandler inside HandleData");
      switch(type){
        case CondorDataType.ClassAdsRequest :
          ClassAdStructAttr[][] classads = _csi.GetStartdAds("(START)&& BRUNET_ADDRESS == \"" + _node.Address.ToString().Substring(12) + "\"");
          byte[] startd_ba;
          if(classads == null){
            Console.WriteLine("Decentralized Debug: CondorDataHandler inside HandleData returned classads is null");
            return;
          }
          try{
            foreach(ClassAdStructAttr[] casa_array in classads){
              startd_ba = CondorSoapInterface.ConvertToByteArrayList(casa_array);
              startd_ba[0] = (byte)CondorDataType.ClassAdsUpdate;
              return_path.Send(new CopyList(CDH, MemBlock.Reference(startd_ba)));
            }
//            _csi.InsertCoordinate(classads[0], _node);
          }catch(Exception e){
            Console.WriteLine("Decentralized Debug: No valid startd classads returned with {0}", e);
            return;
          }
          break;
        case CondorDataType.ClassAdsUpdate :
          rest = payload.Slice(1);
          try{
            classad_al = AdrConverter.Deserialize(rest) as ArrayList;
          }catch(Exception e){
            Console.WriteLine("Decentralized Debug: returned object deserialize failed : {0}", e);
            return;
          }
          _csi.insertAd(CondorSoap.ClassAdType.STARTDADTYPE, CondorSoapInterface.ConvertToCasa(classad_al));
          _po.CondorReschedule.Start();
          Console.WriteLine("Decentralized Debug: Done inserting new ads");
          break;
        case CondorDataType.ChangeCondorHost :
          rest = payload.Slice(1);
          try{
            ChangeCondorHost(AdrConverter.Deserialize(rest) as string);
            byte[] send_ack_byte = new byte[1]; 
            send_ack_byte[0] = (byte)CondorDataType.ChangeHostCmdAck;
            return_path.Send(new CopyList(CDH, MemBlock.Reference(send_ack_byte)));

          }catch(Exception e){
            Console.WriteLine(e);
          }
          break;
        case CondorDataType.CondorHostToOriginal :
          Console.WriteLine("Decentralized Debug: within condor host roll back handlerpc");
          rest = payload.Slice(1);
          try{
            RollbackCondorHost(AdrConverter.Deserialize(rest) as string);
          }catch(Exception e){
            Console.WriteLine(e);
          }
          break;
        case CondorDataType.ChangeHostCmdAck :
          _po.CondorReschedule.Start();
          break;
        default:
          break;
      }
    }
    
    public void SendMessage(Address remote_addr, ICopyable data) {
      AHExactSender sender = new AHExactSender(_node, remote_addr);
      sender.Send(new CopyList(CDH, data));
    }

    protected void ChangeCondorHost(string host){
      host = host.Trim(new char[] {'\n'});
      _po.CondorConfigVal.StartInfo.Arguments = "-startd -rset \"CONDOR_HOST = " + host + "\"";
      _po.CondorConfigVal.Start();
      _po.CondorConfigVal.WaitForExit();
      _po.CondorReconfig.Start();
      Console.WriteLine("Decentralized Debug: CONDOR_HOST changed to {0}", host);
      if(File.Exists(_node.Address.ToString().Substring(12)) == false){
        using(StreamWriter  sw = new StreamWriter(_node.Address.ToString().Substring(12))){
          sw.Write("MyType = \"Query\"\nTargetType = \"Machine\"\nRequirements = BRUNET_ADDRESS == \"" + _node.Address.ToString().Substring(12) + "\"");
        }
      }
      _po.CondorAdvertise.StartInfo.Arguments = "INVALIDATE_STARTD_ADS " + _node.Address.ToString().Substring(12);
      _po.CondorAdvertise.Start();
    }

    protected void RollbackCondorHost(string msg_sender){
      _po.CondorConfigVal.StartInfo.Arguments = "-startd CONDOR_HOST";
      _po.CondorConfigVal.StartInfo.RedirectStandardOutput = true;
      _po.CondorConfigVal.StartInfo.UseShellExecute = false;
      _po.CondorConfigVal.Start();
      string cur_condor_host = _po.CondorConfigVal.StandardOutput.ReadToEnd();
      _po.CondorConfigVal.WaitForExit();
      cur_condor_host = cur_condor_host.Trim(new char[] {'\n'});
      Console.WriteLine("Decentralized Debug: RollbackCondorHost : current condor_host = {0}  msg_sender = {1}", cur_condor_host, msg_sender);
      _po.CondorConfigVal.StartInfo.RedirectStandardOutput = false;
      _po.CondorConfigVal.StartInfo.UseShellExecute = true;
      if(String.Compare(cur_condor_host, msg_sender, true) == 0){
        Console.WriteLine("Decentralized Debug: host roll back to the original");
        _po.CondorConfigVal.StartInfo.Arguments = "-startd -rset \"CONDOR_HOST = " + _condor_host + "\"";
        _po.CondorConfigVal.Start();
        _po.CondorConfigVal.WaitForExit();
        _po.CondorReconfig.Start();
      }
    }
  }

  public class CondorSoapInterface{
    protected condorCollector collector;
    protected condorSchedd schedd;

    public CondorSoapInterface(int sched_port, int collecor_port){
      collector = new condorCollector();
      schedd = new condorSchedd();
      collector.Url = "http://127.0.0.1:" + collecor_port;
      schedd.Url = "http://127.0.0.1:" + sched_port;  
    }

    public CondorSoapInterface():this(45781, 9618){}

    public ClassAdStructAttr[][] GetStartdAds(string constraint){
      return collector.queryStartdAds(constraint);
    }

    public ClassAdStructAttr[][] GetJobAds(string constraint){
      ClassAdStructArrayAndStatus jobads = schedd.getJobAds(null, constraint);
      return jobads.classAdArray;
    }
    
    public ClassAdStructAttr[][] GetAllAds(string constraint){
      return collector.queryAnyAds(constraint);
    }

    public Status insertAd(ClassAdType classad_type, ClassAdStructAttr[] classad){
      return collector.insertAd(classad_type, classad);
    }

    public ClassAdStructAttr[] GetLocalStartdAds(string constraint, string local_brunet_address){
      Console.WriteLine("Decentralized Debug: In the getLocalStartdAds");
      ClassAdStructAttr[][] csa = GetStartdAds(constraint);
      if(csa.Length == 0){
        return null;
      }
      
      return csa[0];
    }

    public static Hashtable ConvertToHashtable(ClassAdStructAttr[] csa){
      Hashtable ht = new Hashtable();
      foreach(ClassAdStructAttr element in csa){
        try{
          if(element.type == ClassAdAttrType.STRINGATTR){
            ht[element.name] = ("\"" + element.value + "\"");
          }else{
            ht[element.name] = element.value;
          }
        }catch(Exception e){
          Console.WriteLine(e);
          continue;
        }
      }
      return ht;
    }

    public static string ConvertToClassAdStyle(ClassAdStructAttr[] csa){
      string one_element;
      string result = "";
      foreach(ClassAdStructAttr csa_element in csa){
        one_element = (csa_element.type == ClassAdAttrType.STRINGATTR ? csa_element.name + " = " + "\"" + csa_element.value + "\"\n" : csa_element.name + " = " + csa_element.value + "\n");
        result += one_element;
      }
      return result;
    }

    public static ClassAdStructAttr[] ConvertToCasa(ArrayList al){
      ClassAdStructAttr[] casa_array = null;
      try{
        casa_array = new ClassAdStructAttr[al.Count/3];
        for(int i = 0; i<al.Count/3; i++){
          casa_array[i] = new ClassAdStructAttr();
          casa_array[i].name = al[3*i] as string;
          casa_array[i].value = al[3*i+1] as string;
          casa_array[i].type = (ClassAdAttrType)al[3*i+2];
        }
      }catch(Exception e){
        Console.WriteLine(e);
      }
      return casa_array;
    }

    public static ArrayList ConverToArrayList(ClassAdStructAttr[] csa){
      ArrayList al = new ArrayList();
      foreach(ClassAdStructAttr casa in csa){
        al.Add(casa.name as string);
        al.Add(casa.value as string);
        al.Add((int)casa.type);
      }
      return al;
    }

    public static byte[] ConvertToByteArrayList(ClassAdStructAttr[] csa){
      ArrayList al = new ArrayList();
      foreach(ClassAdStructAttr casa in csa){
        al.Add((string)casa.name);
        al.Add((string)casa.value);
        al.Add((int)casa.type);
      }
      MemoryStream ms = new MemoryStream();
      int serialized = AdrConverter.Serialize(al, ms);
      byte[] sbuf = new byte[serialized + 1];
      int s2count = AdrConverter.Serialize(al, sbuf, 1);   // 0th byte array should indicate packet kind

      return sbuf;
    }

    public bool InsertCoordinate(Node node){
      Channel ret = new Channel(1);
      ret.CloseEvent += delegate(object o, EventArgs args) {
        RpcResult info_res = (RpcResult) ret.Dequeue();
        Hashtable ht = (Hashtable) info_res.Result;
        foreach(DictionaryEntry de in ht){
          if((string)de.Key == "geo_loc"){
            try{
              string location = de.Value as string;
              if(location != null){
                string[] sep = location.Split(new char[] {','});
                if(sep.Length == 2){
                  Process p = new Process();
                  p.StartInfo.FileName = "condor_config_val";
                  p.StartInfo.CreateNoWindow = true;
                  p.StartInfo.Arguments = "-startd -rset \"COORDINATE = \\\"" + location + "\\\"\"";
                  p.Start();
                  p.WaitForExit();
                  p.StartInfo.Arguments = "-startd -rset \"STARTD_ATTRS = \\$(STARTD_ATTRS) COORDINATE LATITUDE LONGITUDE BRUNET_ADDRESS\"";
                  p.Start();
                  p.WaitForExit();
                  p.StartInfo.FileName = "condor_reconfig";
                  p.StartInfo.CreateNoWindow = true;
                  p.StartInfo.Arguments = null;
                  p.Start();
                }
              }
            }catch(Exception e){
              Console.WriteLine(e);
            }
          }
        }
      };
      node.Rpc.Invoke(node, ret, "Information.Info");
      return true;
    }
    
    public bool InsertCoordinate(ClassAdStructAttr[] casa, Node node){
      foreach(ClassAdStructAttr casa_entry in casa){
        if(String.Compare(casa_entry.name, "COORDINATE", true) == 0){
          if(!casa_entry.value.Equals(",")){
            return true;
          }
        }
      }
      
      return InsertCoordinate(node);
    }

  }

  public class ProcessObject{
    public readonly Process CondorReconfig;
    public readonly Process CondorReschedule;
    public readonly Process CondorConfigVal;
    public readonly Process CondorAdvertise;

    public ProcessObject(){
      CondorReconfig = new Process();
      CondorReschedule = new Process();
      CondorConfigVal = new Process();
      CondorAdvertise = new Process();
      CondorReconfig.StartInfo.FileName = "condor_reconfig";
      CondorReconfig.StartInfo.CreateNoWindow = true;
      CondorReschedule.StartInfo.FileName = "condor_reschedule";
      CondorReschedule.StartInfo.CreateNoWindow = true;
      CondorConfigVal.StartInfo.FileName = "condor_config_val";
      CondorConfigVal.StartInfo.CreateNoWindow = true;
      CondorAdvertise.StartInfo.FileName = "condor_advertise";
      CondorAdvertise.StartInfo.CreateNoWindow = true;

    }
  }

  public class ClassAdsDht : CondorSoapInterface{
    protected ArrayList _cur_startd_classads;
    protected string _classads_dht_key;
    protected long _dht_inserted_tick;
    public readonly int DhtTtl;
    public readonly string BrunetAddress;
    public static string StartdDhtKeyString;
    protected Node _node;
    
    public ClassAdsDht(Node node):base(){
      DhtTtl = 3600; // dht ttl = 1 hour
      _classads_dht_key = null;
      _dht_inserted_tick = 0;
      BrunetAddress = node.Address.ToString();
      StartdDhtKeyString = "StartdClassAdsDhtKey";
      _node = node;
    }
//It needs to be modified for correct DHT operation
    public ArrayList InsertClassAds(ArrayList classads){
      ArrayList ret_al = null;
      
      if(_classads_dht_key == null || IsTimeToAct()){
        if(true == ReinsertClassAds(classads)){
          ret_al = new ArrayList();
          _cur_startd_classads = classads;
          _dht_inserted_tick = DateTime.UtcNow.Ticks;
          ret_al.Add(StartdDhtKeyString);
          ret_al.Add(_classads_dht_key);
          ret_al.Add((int)CondorSoap.ClassAdAttrType.STRINGATTR);
        }else{  //DHT insert failed
          _classads_dht_key = null;
          _cur_startd_classads = null;
          ret_al = classads;
        }
      }else{  //Dht entry already exist
        ret_al = CreateNewEntry(classads);
      }
      return ret_al;
    }
    
    private bool ReinsertClassAds(ArrayList classads){   //check return type
      _classads_dht_key = BrunetAddress + DateTime.UtcNow.Ticks.ToString();
      ArrayList classad_al = new ArrayList();
      Channel ret_queue = new Channel(1);
      for(int i = 0; i<classads.Count/3; i++){
        classad_al.Add(System.Text.Encoding.UTF8.GetBytes((string)classads[3*i]));
        classad_al.Add(System.Text.Encoding.UTF8.GetBytes((string)classads[3*i+1]));
        classad_al.Add(BitConverter.GetBytes((int)classads[3*i+2]));
      }

      byte[] dht_value = (byte[])classad_al.ToArray(typeof(byte[]));
      return true; //check return type
//      return _node.Rpc.Invoke(_node, ret_queue, "DhtClient.Put", (byte[])System.Text.Encoding.UTF8.GetBytes(_classads_dht_key), dht_value, DhtTtl);
    }
    
    private ArrayList CreateNewEntry(ArrayList classads){
      string key, value;
      if(_cur_startd_classads == null){
        Console.WriteLine("Decentralized Debug: System says there is a startd dht entry, but there isn't");
        return classads;
      }

      ArrayList ret_al = new ArrayList();
      ret_al.Add(StartdDhtKeyString);
      ret_al.Add(_classads_dht_key);
      ret_al.Add((int)CondorSoap.ClassAdAttrType.STRINGATTR);

      for(int i = 0; i<classads.Count/3; i++){
        key = classads[3*i] as string;
        for(int j = 0; j<_cur_startd_classads.Count/3; j++){
          if(String.Compare(key, _cur_startd_classads[3*j] as string, true)== 0){
            value = classads[3*i + 1] as string;
            if(String.Compare(value, _cur_startd_classads[3*j + 1] as string, true) != 0){
              ret_al.Add(key);
              ret_al.Add(value);
              ret_al.Add((int)classads[3*i+2]);
            }
          }
        }
      }

      return ret_al;
    }

    private bool IsTimeToAct(){
      long elapsed_ticks = DateTime.UtcNow.Ticks-_dht_inserted_tick>0?DateTime.UtcNow.Ticks-_dht_inserted_tick:DateTime.UtcNow.Ticks+3155378975999999999L-_dht_inserted_tick;
      return (elapsed_ticks/(TimeSpan.TicksPerMillisecond*1000))>DhtTtl ? true:false;
    }
  }

  public class CondorControlAction: IRpcHandler {
    protected Node _node;
    protected CondorSoapInterface _csi;
    public CondorControlAction(Node node){
      _node = node;
      _node.Rpc.AddHandler("CondorControlAction", this);
      _csi = new CondorSoapInterface();
    }
    
    public void HandleRpc(ISender caller, string method, IList arguments, object request_state){
      Process p = new Process();
      StreamWriter sw = null;
      

      object result = new InvalidOperationException("Invalid method");;
      try {
        switch(method) {
          case "Status": 
            ClassAdStructAttr[][] casa_array = _csi.GetStartdAds(null);
            string coordinate = null;
            string state = null;
            ArrayList al_result = new ArrayList();
            try{
              foreach(ClassAdStructAttr[] one_casa in casa_array){
                foreach(ClassAdStructAttr casa_entry in one_casa){
                  if(String.Compare(casa_entry.name, "COORDINATE") == 0){
                    coordinate = casa_entry.value;
                  }else if(String.Compare(casa_entry.name, "State") == 0){
                    state = casa_entry.value;
                  }
                }
                if(coordinate != null && state != null){
                  al_result.Add(coordinate);al_result.Add(state);
                  coordinate=null;state=null;
                }
              }
              result = al_result;
            }catch(Exception e){result = e;}
            

            /*
            p.StartInfo.FileName = "condor_status";
            p.StartInfo.Arguments = "-l";
            p.StartInfo.CreateNoWindow = true;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.UseShellExecute = false;
            p.Start();
            string ret = "";
            string status = p.StandardOutput.ReadToEnd();
            p.WaitForExit();
            string[] split = status.Split(new char [] {'\n', '='});
            Console.WriteLine("split length = {0}", split.Length);
            for(int i=0;i<split.Length;i++){
              if(split[i].Trim().Equals("COORDINATE")){
                ret += split[i+1];
                ret += '\n';
                Console.WriteLine("hit : {0}", ret);
              }else{
                Console.WriteLine(split[i]);
              }
            }
            result = ret;
            */
            
            break;
          case "SubmitJob":
            p.StartInfo.FileName = "condor_submit";
            p.StartInfo.Arguments = "submit";
            p.StartInfo.CreateNoWindow = true;
            try{
              using(sw = new StreamWriter("submit")){
                sw.Write(arguments[0] as string);
              }
              p.Start();
              result = "Succeed to Submit Jobs";
            }catch(Exception e){
              result = e;
            }
            break;
          case "ListEntries":
            break;
          default:
            break;
        }
      }
      catch (Exception e) {
        result = new AdrException(-32602, e);
      }
      _node.Rpc.SendResult(request_state, result);
    }

  }
}

