/*
This program is part of BruNet, a library for the creation of efficient overlay
networks.
Copyright (C) 2008 Kyungyong Lee <kyungyonglee@ufl.edu> University of Florida  
                   P. Oscar Boykin <boykin@pobox.com>, University of Florida

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
//#define BRUNET_SIMULATOR

using System;
using System.Collections;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using condor.classad;
using CondorSoap;

using Brunet.Concurrent;
using Brunet.Messaging;
using Brunet.Services.DecenGrids;

namespace Brunet.Services.MapReduce {

  public class MapReduceDemoRf : MapReduceBoundedBroadcast{
    private readonly Address LocalAddress;
    private Node _node;
/*    
    public interface IRpcGetResInfo : IXmlRpcProxy {
      [XmlRpcMethod]
      object GetResInfo();
    }
*/
    public MapReduceDemoRf(Node n) : base(n){
       LocalAddress = n.Address;
       _node = n;
    }
    public override void Map(Channel q, object map_arg) {
      Hashtable result;
//      string result="<c><a n=\"MyType\"><s>Machine</s></a><a n=\"TargetType\"><s>Job</s></a><a n=\"MyType\"><s>Machine</s></a><a n=\"TargetType\"><s>Job</s></a><a n=\"Name\"><s>kyungyong-desktop</s></a><a n=\"Rank\"><i>1</i></a><a n=\"CpuBusy\"><e>((LoadAvg - CondorLoadAvg) &gt;= 0.500000)</e></a><a n=\"MyCurrentTime\"><i>1255832853</i></a><a n=\"Machine\"><s>kyungyong-desktop</s></a><a n=\"PublicNetworkIpAddr\"><s>&lt;127.0.1.1:51073&gt;</s></a><a n=\"DedicatedScheduler\"><s>DedicatedScheduler@full.host.name</s></a><a n=\"COLLECTOR_HOST_STRING\"><s>central-manager-hostname.your.domain</s></a><a n=\"CondorVersion\"><s>$CondorVersion: 7.2.4 Oct  1 2009 $</s></a><a n=\"CondorPlatform\"><s>$CondorPlatform: I386-LINUX_DEBIAN50 $</s></a><a n=\"SlotID\"><i>1</i></a><a n=\"VirtualMachineID\"><i>1</i></a><a n=\"VirtualMemory\"><i>746980</i></a><a n=\"TotalDisk\"><i>8362604</i></a><a n=\"Disk\"><i>8362604</i></a><a n=\"CondorLoadAvg\"><r>0.000000000000000E+00</r></a><a n=\"LoadAvg\"><r>1.099999994039536E-01</r></a><a n=\"KeyboardIdle\"><i>0</i></a><a n=\"ConsoleIdle\"><i>0</i></a><a n=\"Memory\"><i>1002</i></a><a n=\"Cpus\"><i>1</i></a><a n=\"StartdIpAddr\"><s>&lt;127.0.1.1:51073&gt;</s></a><a n=\"Arch\"><s>INTEL</s></a><a n=\"OpSys\"><s>LINUX</s></a><a n=\"UidDomain\"><s>your.domain</s></a><a n=\"FileSystemDomain\"><s>your.domain</s></a><a n=\"HasIOProxy\"><b v=\"t\"/></a><a n=\"CheckpointPlatform\"><s>LINUX INTEL 2.6.x normal N/A</s></a><a n=\"TotalVirtualMemory\"><i>746980</i></a><a n=\"TotalCpus\"><i>1</i></a><a n=\"TotalMemory\"><i>1002</i></a><a n=\"KFlops\"><i>780303</i></a><a n=\"Mips\"><i>3348</i></a><a n=\"LastBenchmark\"><i>1255832306</i></a><a n=\"TotalLoadAvg\"><r>1.099999994039536E-01</r></a><a n=\"TotalCondorLoadAvg\"><r>0.000000000000000E+00</r></a><a n=\"ClockMin\"><i>1343</i></a><a n=\"ClockDay\"><i>6</i></a><a n=\"TotalSlots\"><i>1</i></a><a n=\"TotalVirtualMachines\"><i>1</i></a><a n=\"StarterAbilityList\"><s></s></a><a n=\"HasVM\"><b v=\"f\"/></a><a n=\"HibernationLevel\"><i>0</i></a><a n=\"HibernationState\"><s>NONE</s></a><a n=\"CanHibernate\"><b v=\"t\"/></a><a n=\"CpuBusyTime\"><i>0</i></a><a n=\"CpuIsBusy\"><b v=\"f\"/></a><a n=\"TimeToLive\"><i>2147483647</i></a><a n=\"State\"><s>Owner</s></a><a n=\"EnteredCurrentState\"><i>1255832301</i></a><a n=\"Activity\"><s>Idle</s></a><a n=\"EnteredCurrentActivity\"><i>1255832301</i></a><a n=\"TotalTimeOwnerIdle\"><i>552</i></a><a n=\"Start\"><e>Scheduler =?= \"DedicatedScheduler@full.host.name\"</e></a><a n=\"Requirements\"><e>(START) &amp;&amp; (IsValidCheckpointPlatform)</e></a><a n=\"IsValidCheckpointPlatform\"><e>(((TARGET.JobUniverse == 1) == FALSE) || ((MY.CheckpointPlatform =!= UNDEFINED) &amp;&amp; ((TARGET.LastCheckpointPlatform =?= MY.CheckpointPlatform) || (TARGET.NumCkpts == 0))))</e></a><a n=\"MaxJobRetirementTime\"><i>0</i></a><a n=\"LastFetchWorkSpawned\"><i>0</i></a><a n=\"LastFetchWorkCompleted\"><i>0</i></a><a n=\"NextFetchWorkDelay\"><i>-1</i></a><a n=\"CurrentRank\"><r>0.000000000000000E+00</r></a><a n=\"MonitorSelfTime\"><i>1255832786</i></a><a n=\"MonitorSelfCPUUsage\"><r>1.665199920535088E-02</r></a><a n=\"MonitorSelfImageSize\"><r>1.785600000000000E+04</r></a><a n=\"MonitorSelfResidentSetSize\"><i>4460</i></a><a n=\"MonitorSelfAge\"><i>0</i></a><a n=\"MonitorSelfRegisteredSocketCount\"><i>2</i></a></c>";
      Hashtable local_classad;

      CondorSoapInterface csi = new CondorSoapInterface();
      try{
        local_classad = CondorSoapInterface.ConvertToHashtable(csi.GetLocalStartdAds(null, LocalAddress.ToString()));
      }catch{
        result = new Hashtable();
        result.Add("count", 1);
        result.Add("condor_startd_execution_error",1);
        Console.WriteLine("condor_execution error");
        q.Enqueue(result);
        return;
      }

      try{
//        result = CheckMatching(rets as string, map_arg);
//        Console.WriteLine(rets as string);
        result = CheckMatching(local_classad, map_arg, q);
      }catch(Exception e){
        result = new Hashtable();
        result.Add("count", 1);
        result.Add("checkmatching_exception_returned", 1);
        Console.WriteLine("checkmatching_exception_returned : " + e);
      }

      if(result != null){
        q.Enqueue(result);
      }
    }
    
    public override void Reduce(Channel q, object reduce_arg, 
                                  object current_result, RpcResult child_rpc) {
      bool done = false;
      int res_count = -1;
      bool sort_ascnd = true;
      object child_result;

      try{
        child_result = child_rpc.Result;
      }catch{  //if timeout occurs.
        q.Enqueue(new Brunet.Collections.Pair<object, bool>(current_result, false));
        return;
      }

      
      Hashtable red_arg = reduce_arg as Hashtable;
      if(red_arg.ContainsKey("num_res")){
        res_count = (int)red_arg["num_res"];
      }
      if(red_arg.ContainsKey("sort_descending")){
        sort_ascnd = false;
      }

      if (current_result == null) {
        q.Enqueue(new Brunet.Collections.Pair<object, bool>(child_result, done));
        return;
      }
   
      Hashtable my_entry = current_result as Hashtable;
      Hashtable value = child_result as Hashtable;
      Hashtable error_entry = CombineErrorEntry(ref my_entry, ref value);
      my_entry = ConcatenateTwoHt(my_entry, value);
      done = OrganizeResult(ref my_entry, res_count, sort_ascnd);
      done = (red_arg.ContainsKey("first_fit") ? done:false);
      my_entry = ConcatenateTwoHt(my_entry, error_entry);
      q.Enqueue(new Brunet.Collections.Pair<object, bool>(my_entry, done));
    }

//    private Hashtable CheckMatching(string res_info, object map_arg){
    private Hashtable CheckMatching(Hashtable res_info, object map_arg, Channel queue){
      Random r = new Random((int)DateTime.UtcNow.Ticks);
      Hashtable result = new Hashtable();
      result.Add("count", 1);
      ClassAdParser req_parser = null;
      ClassAdParser rank_parser = null;

      bool coordinate_check = false;
      Expr req_check = null;
      Expr rank_check = null;
      RecordExpr user_req;
      Hashtable ma = map_arg as Hashtable;
      if(ma != null){
        if(ma.ContainsKey("requirements")){
          string req = ma["requirements"] as string;
          coordinate_check = (req.Contains("latitude")  || req.Contains("longitude"))? true : false;
          req_parser = new ClassAdParser(req);
          req_check = req_parser.parse();
          user_req = new RecordExpr();
          user_req.insertAttribute("requirements", req_check);
          if(ma.ContainsKey("rank")){
            rank_parser = new ClassAdParser((string)ma["rank"]);
            rank_check = rank_parser.parse();
            user_req.insertAttribute("rank", rank_check);
          }
          else{
            user_req.insertAttribute("rank", Constant.getInstance(1));
          }
        }
        else{
          result.Add("invalid_map_argument", 1);
          return result;
        }
      }
      else{
        result.Add("invalid_map_argument", 1);
        return result;
      }

      ClassAdObject data_info = new ClassAdObject(res_info);
      RecordExpr match_expr = data_info.MatchingData;
      match_expr.insertAttribute("requirements", Constant.getInstance(true));
      match_expr.insertAttribute("rank", Constant.getInstance(1));
      match_expr.insertAttribute("Temp", Constant.getInstance(r.Next(68,104)));
      match_expr.insertAttribute("Machine", Constant.getInstance("advanta.microsoft.com"));
      int[] rank_value = null;
      
      if(!coordinate_check){
        rank_value = ClassAd.match(user_req, match_expr);
        if(rank_value != null){
          coordinate_check = true;
        }
        else{
          Console.WriteLine("classad_nomatching");
          result.Add("classad_nomatching", 1);
        }
      }
      
      if(coordinate_check){
        Channel ret = new Channel(1);
        _node.Rpc.Invoke(_node, ret, "Information.Info");
        string comb_string = LocalAddress.ToString() + " ";
         ret.CloseEvent += delegate(object o, EventArgs args) {
          RpcResult info_res = (RpcResult) ret.Dequeue();
          Hashtable ht = (Hashtable) info_res.Result;
          foreach(DictionaryEntry de in ht){
            if((string)de.Key == "geo_loc"){
              Console.WriteLine("inside geo_loc = " + de.Value);
              try{
                string location = de.Value as string;
                string[] parts = location.Split(',');
                comb_string += Double.Parse(parts[0]).ToString();
                comb_string += " ";
                comb_string += Double.Parse(parts[1]).ToString();
                match_expr.insertAttribute("latitude", Constant.getInstance(Double.Parse(parts[0])));
                match_expr.insertAttribute("longitude", Constant.getInstance(Double.Parse(parts[1])));
//                Console.WriteLine("latitude = " + Double.Parse(parts[0]) + " longitude = " + Double.Parse(parts[1]));
              }catch{
//                comb_string += "29.65 -82.32";
                comb_string += "47.58 -122.12"; 
              }
            }
          }
          
          rank_value = ClassAd.match(user_req, match_expr);
          
          if(rank_value != null){
            Console.WriteLine("classad_matching");
            result.Add(comb_string, rank_value[0]);  //geo_loc(0:lat, 1:long, 2:rank)
          }
          else{
            Console.WriteLine("classad_nomatching");
            result.Add("classad_nomatching", 1);
          }
          queue.Enqueue(result);
        };
        return null;
      }

      return result;
    }

    private Hashtable ConcatenateTwoHt(Hashtable ht1, Hashtable ht2){
      if(ht1 != null && ht2 != null){
        foreach(DictionaryEntry de in ht2){
          ht1.Add(de.Key, de.Value);
        }
        return ht1;
      }
      else if(ht1 != null && ht2 == null){
        return ht1;
      }
      else if(ht1 == null && ht2 != null){
        return ht2;
      }      
      else if(ht1 == null && ht2 == null){
        return null;
      }

      return null;
    }

    private bool OrganizeResult(ref Hashtable ht, int num_res, bool ascnd){
      if(ht == null){
        return false;
      }

      int num_removed = ht.Count - num_res;
      if(num_removed <= 0 || ht.Count == 0 || num_res < 0 || num_res == 0){
        return false;
      }

      string[] keys = new string[ht.Count];
      ht.Keys.CopyTo(keys, 0);
      int[] values = new int[ht.Count];
      ht.Values.CopyTo(values, 0);
      
      if(ascnd){
        Array.Sort(values, keys);
      }else{
        Array.Sort(values, keys, new ReverseCompare());
      }
      ht.Clear();
      for(int i=0; i<num_res; i++){
        ht.Add(keys[i], values[i]);
      }
      return true;
    }

    private Hashtable CombineErrorEntry(ref Hashtable ht1, ref Hashtable ht2){
      Hashtable error_entry = new Hashtable();
      KeyBasedCombine("invalid_map_argument", ref ht1, ref ht2, ref error_entry);
      KeyBasedCombine("classad_nomatching", ref ht1, ref ht2, ref error_entry);
      KeyBasedCombine("condor_startd_execution_error", ref ht1, ref ht2, ref error_entry);
      KeyBasedCombine("checkmatching_exception_returned", ref ht1, ref ht2, ref error_entry);
      KeyBasedCombine("count", ref ht1, ref ht2, ref error_entry);
      return error_entry;
    }
    
    private void KeyBasedCombine(string key, ref Hashtable ht1, ref Hashtable ht2, ref Hashtable error_entry){
      if(ht1 != null && ht2 != null){
        if(ht1.ContainsKey(key)&& ht2.ContainsKey(key)){
          error_entry.Add(key,(int)ht1[key]+(int)ht2[key]);
          ht1.Remove(key);
          ht2.Remove(key);
          return;
        }
        else if(ht1.ContainsKey(key)){
          error_entry.Add(key,(int)ht1[key]);
          ht1.Remove(key);
          return;
        }
        else if(ht2.ContainsKey(key)){
          error_entry.Add(key,(int)ht2[key]);
          ht2.Remove(key);
          return;
        }
      }
      else if(ht1 != null){
        if(ht1.ContainsKey(key)){
          error_entry.Add(key,(int)ht1[key]);
          ht1.Remove(key);
          return;
        }
      }
      else if(ht2 != null){
        if(ht2.ContainsKey(key)){
          error_entry.Add(key,(int)ht2[key]);
          ht2.Remove(key);
          return;
        }
      }
      
      return;
    }
  }

  public class ReverseCompare : IComparer{
     public int Compare(Object x, Object y){
         return -((IComparable)x).CompareTo (y);
     }
  }
}

