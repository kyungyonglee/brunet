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
using System.Collections.Specialized;

using Brunet.Concurrent;
using Brunet.Messaging;

namespace Brunet.Services.MapReduce {

  public class MapReduceGetNeighborInfo: MapReduceBoundedBroadcast {
    public MapReduceGetNeighborInfo(Node n): base(n) {}
    public override void Map(Channel q, object map_arg) {
      Channel ret = new Channel(1);
      long begin_ticks = DateTime.UtcNow.Ticks;
#if BRUNET_SIMULATOR
      System.Random r = new Random((int)begin_ticks);
      int int_r;
#endif


      ret.CloseEvent += delegate(object o, EventArgs args) {
        try {
          int size = 0;
          Hashtable list = new Hashtable();
          Channel queue = (Channel) o;
          RpcResult result = (RpcResult) queue.Dequeue();
          Hashtable ht = (Hashtable) result.Result;
          string self_addr = (string)ht["self"];
          ht.Remove("self");
          ht.Remove("shortcut");
          ht.Add("cons", 0);
#if BRUNET_SIMULATOR
          int_r = r.Next(10000,1000000);
          if(int_r%10 == 0){
            long current_tick = DateTime.UtcNow.Ticks;
            DateTime.SetTime((current_tick/TimeSpan.TicksPerMillisecond) + (long)int_r);
            Console.WriteLine("Elapsed msec  is {0}", ((DateTime.UtcNow.Ticks - current_tick) / TimeSpan.TicksPerMillisecond));
          }
#endif          
          ht.Add("map_time", (DateTime.UtcNow.Ticks - begin_ticks)/TimeSpan.TicksPerMillisecond);
          ht.Add("remain_op", 4);  // remaining operation for left2, left1, right1, right 2
          list.Add(self_addr, ht);
          q.Enqueue(list);
        }catch{
          Hashtable exception_res = new Hashtable();
          Hashtable result = new Hashtable();
          result.Add("map_time", (DateTime.UtcNow.Ticks - begin_ticks)/TimeSpan.TicksPerMillisecond);
          exception_res.Add("exception", result);   //to make it same structure with normal result
          q.Enqueue(exception_res);
        }
      };
      _node.Rpc.Invoke(_node, ret, "sys:link.GetNeighbors");
    }
    
    public override void Reduce(Channel q, object reduce_arg, 
                                  object current_result, RpcResult child_rpc) {

      bool done = false;
      int old_count = 0;
      object child_result;

      try{
        child_result = child_rpc.Result;
      }catch(Exception e){  //if timeout occurs.
        q.Enqueue(new Brunet.Collections.Pair<object, bool>(current_result, false));
        return;
      }

      if (current_result == null) {
        q.Enqueue(new Brunet.Collections.Pair<object, bool>(child_result, done));
        return;
      }
   
      Hashtable new_list = new Hashtable();
      Hashtable cur_hash_res = current_result as Hashtable;
      Hashtable child_hash_res = child_result as Hashtable;
      
      if((cur_hash_res == null) || (child_hash_res == null)){
        if(cur_hash_res != null){
          q.Enqueue(new Brunet.Collections.Pair<object, bool>(cur_hash_res, done));
        }
        else if(child_hash_res != null){
          q.Enqueue(new Brunet.Collections.Pair<object, bool>(child_hash_res, done));
        }
        return;
      }

      long red_begin_tick = DateTime.UtcNow.Ticks;
      //to prevent same key("ordered_result") from Adding two times.
      if((cur_hash_res.ContainsKey("ordered_result")) && (child_hash_res.ContainsKey("ordered_result"))){
        int node_count, consistency;
        long max_map_time;
        Hashtable cur_ordered_res = (Hashtable)cur_hash_res["ordered_result"];
        Hashtable child_ordered_res = (Hashtable)child_hash_res["ordered_result"];
        node_count = (cur_ordered_res.ContainsKey("count"))?(int)cur_ordered_res["count"]:0;
        consistency = (cur_ordered_res.ContainsKey("cons"))?(int)cur_ordered_res["cons"]:0;
        max_map_time = (cur_ordered_res.ContainsKey("max_map_time"))?(long)cur_ordered_res["max_map_time"]:0L;
        max_map_time = (max_map_time > (long)child_ordered_res["max_map_time"])?max_map_time:(long)child_ordered_res["max_map_time"];
        node_count += (int)child_ordered_res["count"];
        consistency += (int)child_ordered_res["cons"];
        cur_ordered_res["count"] = node_count;
        cur_ordered_res["cons"] = consistency;
        cur_ordered_res["max_map_time"] = max_map_time;
        child_hash_res.Remove("ordered_result");
      }
      
      if((cur_hash_res.ContainsKey("exception")) && (child_hash_res.ContainsKey("exception"))){
        HandleBothExcpetion(ref cur_hash_res);
      }

      foreach(DictionaryEntry de in cur_hash_res){
        new_list.Add(de.Key,de.Value);
      }
      
      foreach(DictionaryEntry de in child_hash_res){
        new_list.Add(de.Key,de.Value);
      }

      new_list = GetConsAndTime(new_list);

      q.Enqueue(new Brunet.Collections.Pair<object, bool>(new_list, done));
    }
    
    private Hashtable GetConsAndTime(Hashtable input){
      ArrayList delete_entry = new ArrayList();
      int self_remain_op, left_remain_op;
      if(!input.ContainsKey("ordered_result")){
        AddOrderedResult(ref input);
      }
      
      foreach(DictionaryEntry de in input){
        string self_addr = (string)de.Key;
        if(self_addr.Equals("exception")){
          HadnleExceptionResult((Hashtable)de.Value, ref input);
          continue;
        }
        if(self_addr.Equals("ordered_result")){
          continue;
        }
        
        Hashtable self_info = (Hashtable)de.Value;
        if(self_info.ContainsKey("left2")){   //left2 does not exist means that the entry's left2 has already been calculated
          string left2_addr = (string)self_info["left2"];
          if(input.ContainsKey(left2_addr)){   // input hashtable does not have left2 means that left2's information has not arrived yer.
            Hashtable left2_info = (Hashtable)input[left2_addr];  
            if(left2_info.Contains("right2")){
              if((string)left2_info["right2"] == self_addr){  //its left2 address and left2's right2 address is same
                int cons = (int)self_info["cons"];
                cons+=1;
                self_info["cons"] = cons;
                self_info.Remove("left2");   //remove its own left2 address
                left2_info.Remove("right2");  //remove left2's right2 address to indicate hit
                UpdateRemainOp(self_addr, ref self_info,ref delete_entry);
                UpdateRemainOp(left2_addr, ref left2_info, ref delete_entry);
              }
              else{    //left2's right2 information exists, but it does not match, so simpy remove its own left2 entry
                self_info.Remove("left2");
                UpdateRemainOp(self_addr, ref self_info,ref delete_entry);
              }
            }    
            else{       //left2 entry does not have right2 information means that right2 information has already matched to the other node, so consistency misses and simply remove left2 neighbor information
              self_info.Remove("left2");
              UpdateRemainOp(self_addr, ref self_info,ref delete_entry);
            }
          }
        }

        if(self_info.ContainsKey("left")){   //left1 does not exist means that the entry's left1 has already been calculated
          string left1_addr = (string)self_info["left"];
          if(input.ContainsKey(left1_addr)){   // input hashtable does not have left1 means that left1's information has not arrived yer.
            Hashtable left1_info = (Hashtable)input[left1_addr];  
            if(left1_info.Contains("right")){
              if((string)left1_info["right"] == self_addr){  //its left1 address and left1's right1 address is same
                int cons = (int)self_info["cons"];
                cons+=1;
                self_info["cons"] = cons;
                self_info.Remove("left");   //remove its own left1 address
                left1_info.Remove("right");  //remove left1's right1 address to indicate hit
                UpdateRemainOp(self_addr, ref self_info, ref delete_entry);
                UpdateRemainOp(left1_addr, ref left1_info, ref delete_entry);
              }
              else{    //left1's right1 information exists, but it does not match, so simpy remove its own left1 entry
                self_info.Remove("left");
                UpdateRemainOp(self_addr, ref self_info, ref delete_entry);
              }
            }    
            else{       //left1 entry does not have right1 information means that right1 information has already matched to the other node, so consistency misses and simply remove left1 neighbor information
              self_info.Remove("left");
              UpdateRemainOp(self_addr, ref self_info, ref delete_entry);
            }
          }
        }
      }

      UpdateList(ref input, delete_entry);      
      return input;
    }

    private int GetEntryByteSize(Hashtable input){
      int ret = 0;
      string temp = null;
      foreach(DictionaryEntry de in input){
        temp = de.Value as string;
        if(temp != null){
          ret += temp.Length;
        }
        temp = de.Key as string;
        if(temp != null){
          ret += temp.Length;
        }
      }

      return ret;
    }

    private void SetEntrySize(Hashtable input){
      foreach(DictionaryEntry de in input){
        Hashtable ht = de.Value as Hashtable;
        string key_string = de.Key as string;
        int ht_size = 0;
        ht_size = (int)ht["size"];
        ht_size += key_string.Length;
        ht_size += GetEntryByteSize(ht);
        ht["size"] = ht_size;
      }
    }

    private void HadnleExceptionResult(Hashtable exception, ref Hashtable input){
      int count, cons;
      long max_map_time;
      
      Hashtable value = (Hashtable)input["ordered_result"];
      count = (value.ContainsKey("count"))?(int)value["count"]:0;
      value["count"] = count + 1;
      max_map_time = (long)value["max_map_time"];
      value["max_map_time"] = (max_map_time < (long)exception["map_time"]) ? (long)exception["map_time"] : max_map_time;
    }

    private void UpdateRemainOp(string addr, ref Hashtable input, ref ArrayList del_entry){
      int remain_op;
      remain_op = (int)input["remain_op"];
      remain_op--;
      if(remain_op == 0){
        del_entry.Add(addr);
      }
      input["remain_op"] = remain_op;
    }

    private void UpdateList(ref Hashtable input, ArrayList delete_list){
      int consistency, del_cons, count;
      long max_map_time;

      if(input.ContainsKey("exception")){
        input.Remove("exception");
      }
      
      Hashtable ordered_result = (Hashtable)input["ordered_result"];  //ordered result is added at the caller, so we don't have to check ContainsKey
      count = (int)ordered_result["count"];
      consistency = (int)ordered_result["cons"];
      max_map_time = (long)ordered_result["max_map_time"];
      foreach(string s in delete_list){
        Hashtable value = (Hashtable)input[s];
        count += 1;
        max_map_time = (max_map_time > (long)value["map_time"]) ? max_map_time:(long)value["map_time"];
        del_cons = (int)value["cons"];
        consistency += del_cons;
        input.Remove(s);
      }
      ordered_result["count"] = count;
      ordered_result["cons"] = consistency;
      ordered_result["max_map_time"] = max_map_time;
    }

    private void HandleBothExcpetion(ref Hashtable current){
      Hashtable exception = (Hashtable)current["exception"];
      if(!current.ContainsKey("ordered_result")){
        AddOrderedResult(ref current);
      }
      HadnleExceptionResult(exception,ref current);
      current.Remove("exception");
    }

    private void AddOrderedResult(ref Hashtable input){
      Hashtable ordered_result = new Hashtable();
      ordered_result.Add("max_map_time", 0L);
      ordered_result.Add("count", 0);
      ordered_result.Add("cons", 0);
      input.Add("ordered_result", ordered_result);
    }
  }
}

