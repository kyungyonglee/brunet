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
using System.Collections.Generic;
using condor.classad;
using CondorSoap;

using Brunet.Concurrent;
using Brunet.Messaging;
using Brunet.Services.DecenGrids;

namespace Brunet.Services.MapReduce {

  public class MapReduceMatchmaking : MapReduceBoundedBroadcast{
    public readonly Address LocalAddress;
    protected Node _node;
    
    public MapReduceMatchmaking(Node n) : base(n){
       LocalAddress = n.Address;
       _node = n;
    }
    public override void Map(Channel q, object map_arg) {
      Hashtable result = new Hashtable();
      ClassAdStructAttr[][] classads;
      string job_ads = map_arg as string;
      ClassAdsDht cad = new ClassAdsDht(_node);
      
      try{
        job_ads = job_ads.Insert(job_ads.IndexOf("Requirements = ") + 15, "(START)&& BRUNET_ADDRESS == \"" + _node.Address.ToString().Substring(12) + "\" && ");
//        Console.WriteLine("Decentralized Debug: MapReduceCondorMAtchmaking Map function: req = " + job_ads);
        classads = cad.GetStartdAds(job_ads);
      }catch{
        result.Add("condor_startd_execution_error",1);
        Console.WriteLine("condor_execution error");
        q.Enqueue(result);
        return;
      }

      Hashtable result_hash = new Hashtable();
      if(classads.Length != 0){
        Console.WriteLine("classad_matching");
        ArrayList al = new ArrayList();
        foreach(ClassAdStructAttr casa in classads[0]){
          al.Add(casa.name as string);
          al.Add(casa.value as string);
          al.Add((int)casa.type);
        }
        ArrayList result_al = cad.InsertClassAds(al);
        result.Add(_node.Address.ToString(), result_al);
      }
      else{
        Console.WriteLine("classad_nomatching");
        result.Add("classad_nomatching", 1);
      }
      q.Enqueue(result);
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
      
      if (current_result == null) {
        q.Enqueue(new Brunet.Collections.Pair<object, bool>(child_result, done));
        return;
      }

      
      Hashtable red_arg = reduce_arg as Hashtable;
      if(red_arg.ContainsKey("num_res")){
        res_count = (int)red_arg["num_res"];
        Console.WriteLine("Decentralized Debug: number of resources = {0}", res_count);
      }
      if(red_arg.ContainsKey("sort_descending")){
        sort_ascnd = false;
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

    private Hashtable Matchmaking(ClassAdStructAttr[] startd_classad, object map_arg){
      Hashtable result = new Hashtable();
      ClassAdObject data_ads = new ClassAdObject(startd_classad);
      ClassAdObject query_ads = null;
      
      try{
        Hashtable ma = map_arg as Hashtable;
        query_ads = new ClassAdObject(ma); 
      }catch{
        result.Add("invalid_map_argument", 1);
        return result;
      }

      int[] rank_value = null;
      rank_value = ClassAdsMatchmaking.CheckMatching(query_ads, data_ads);
      
      if(rank_value != null){
        Console.WriteLine("classad_matching: {0}", rank_value[0]);
        result.Add(startd_classad, rank_value[0]);  //rank[0] = query rank, rank[1] = data rank
      }
      else{
        Console.WriteLine("classad_nomatching");
        result.Add("classad_nomatching", 1);
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
      if(num_removed < 0 || ht.Count == 0 || num_res <= 0){
        return false;
      }

      return true;
/*
      Hashtable[] keys = new Hashtable[ht.Count];
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
      */
    }

    private Hashtable CombineErrorEntry(ref Hashtable ht1, ref Hashtable ht2){
      Hashtable error_entry = new Hashtable();
      KeyBasedCombine("invalid_map_argument", ref ht1, ref ht2, ref error_entry);
      KeyBasedCombine("classad_nomatching", ref ht1, ref ht2, ref error_entry);
      KeyBasedCombine("condor_startd_execution_error", ref ht1, ref ht2, ref error_entry);
      KeyBasedCombine("checkmatching_exception_returned", ref ht1, ref ht2, ref error_entry);
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
}


