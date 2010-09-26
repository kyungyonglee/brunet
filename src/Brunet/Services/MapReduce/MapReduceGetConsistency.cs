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

using Brunet.Concurrent;
using Brunet.Messaging;
using Brunet.Connections;
using Brunet.Symphony;

namespace Brunet.Services.MapReduce {

  public class MapReduceGetConsistency: MapReduceBoundedBroadcast {
    MrTaskAutoRunner _periodic_check;
    public MapReduceGetConsistency(Node n): base(n) {
//      _periodic_check = new MrTaskAutoRunner(this.ToString(), n, 10, 10000); 
    }
    public override void Map(Channel q, object map_arg) {
      Channel l1_returns = new Channel(1);
      Channel l2_returns = new Channel(1);
      Connection left1 = null;
      bool another_done = new bool();
      double another_res = new double();
      object sync = new object();
      long begin_ticks = DateTime.UtcNow.Ticks;
#if BRUNET_SIMULATOR
      System.Random r = new Random((int)begin_ticks);
      int int_r;
#endif

      another_done = false;
      another_res = 0.0;
      l1_returns.CloseEvent += delegate(object o, EventArgs args) {
        double hit = 0.0;
        Address addr = _node.Address;
        Channel queue = (Channel) o;
        try {
          RpcResult res = (RpcResult) queue.Dequeue();
          Hashtable ht = (Hashtable) res.Result;
          Address right = AddressParser.Parse((string) ht["right"]);
          lock(sync){
            if(another_done){
              Hashtable my_entry = new Hashtable();
              if(right.Equals(_node.Address)){
                hit = 1.0;
              }
              my_entry["consistency"] = (another_res + hit) / (2.0);
              my_entry["count"] = 1;
              my_entry["height"] = 1;
#if BRUNET_SIMULATOR
              int_r = r.Next(10000,1000000);
              if(int_r%10 == 0){
                long current_tick = DateTime.UtcNow.Ticks;
                DateTime.SetTime((current_tick/TimeSpan.TicksPerMillisecond) + (long)int_r);
                Console.WriteLine("Elapsed msec  is {0}", ((DateTime.UtcNow.Ticks - current_tick) / TimeSpan.TicksPerMillisecond));
              }
#endif

//              Console.WriteLine("DateTime.UtcNow.Ticks = {0}, begin_Ticks = {1}, diff={2}", DateTime.UtcNow.Ticks,begin_ticks, (DateTime.UtcNow.Ticks - begin_ticks));
              my_entry["map_time"] = (DateTime.UtcNow.Ticks - begin_ticks)/TimeSpan.TicksPerMillisecond;
              q.Enqueue( my_entry );
            }
            else{
              if(right.Equals(_node.Address)){
                another_res = 1.0;
              }
              another_done = true;
            }
          }
        }catch{
          lock(sync){
            HandleRpcException(ref another_done, another_res, q, begin_ticks);
          }
        }
      };
      
      l2_returns.CloseEvent += delegate(object o, EventArgs args) {
        double hit = 0.0;
        Address addr = _node.Address;
        Channel queue = (Channel) o;
        try {
          RpcResult res = (RpcResult) queue.Dequeue();
          Hashtable ht = (Hashtable) res.Result;
          Address right2 = AddressParser.Parse((string) ht["right2"]);
          lock(sync){
            if(another_done){
              Hashtable my_entry = new Hashtable();
              if(right2.Equals(_node.Address)){
                hit = 1.0;
              }
              my_entry["consistency"] = (another_res + hit) / (2.0);
              my_entry["count"]=1;
              my_entry["height"] = 1;
#if BRUNET_SIMULATOR
              int_r = r.Next(10000,1000000);
              if(int_r%10 == 0){
                long current_tick = DateTime.UtcNow.Ticks;
                DateTime.SetTime((current_tick/TimeSpan.TicksPerMillisecond) + (long)int_r);
                Console.WriteLine("Elapsed msec  is {0}", ((DateTime.UtcNow.Ticks - current_tick)/TimeSpan.TicksPerMillisecond));
              }
#endif
//              Console.WriteLine("DateTime.UtcNow.Ticks = {0}, begin_Ticks = {1}, diff={2}", DateTime.UtcNow.Ticks,begin_ticks, (DateTime.UtcNow.Ticks - begin_ticks));
              my_entry["map_time"] = (DateTime.UtcNow.Ticks - begin_ticks)/TimeSpan.TicksPerMillisecond;

              q.Enqueue( my_entry );
            }
            else{
              if(right2.Equals(_node.Address)){
                another_res = 1.0;
              }
              another_done = true;
            }
          }
        }catch{
          lock(sync){
            HandleRpcException(ref another_done, another_res, q, begin_ticks);
          }
        }        
      };

      try {
        left1 = _node.ConnectionTable.GetLeftStructuredNeighborOf((AHAddress)_node.Address);
        if( left1 != null ) {
          AHExactSender sender = new AHExactSender(_node, left1.Address);
          _node.Rpc.Invoke(sender, l1_returns, "sys:link.GetNeighbors");
        }
      }catch {
        lock(sync){
          HandleRpcException(ref another_done, another_res, q, begin_ticks);
        }
      }
      try {
        Connection left2 = _node.ConnectionTable.GetLeftStructuredNeighborOf((AHAddress)left1.Address);
        if( left2 != null ) {
          AHExactSender sender = new AHExactSender(_node, left2.Address);
          _node.Rpc.Invoke(sender, l2_returns, "sys:link.GetNeighbors");
        }
      }catch {
        lock(sync){
          HandleRpcException(ref another_done, another_res, q, begin_ticks);
        }
      }
    }
    
    public override void Reduce(Channel q, object reduce_arg, 
                                  object current_result, RpcResult child_rpc) {

      bool done = false;
      //ISender child_sender = child_rpc.ResultSender;
      //the following can throw an exception, will be handled by the framework
      object child_result;
      long reduce_time;
      
      try{
        child_result = child_rpc.Result;
      }catch(Exception e){  //if timeout occurs.
        q.Enqueue(new Brunet.Collections.Pair<object, bool>(current_result, false));
        return;
      }
      
      //child result is a valid result
      if (current_result == null) {
        q.Enqueue(new Brunet.Collections.Pair<object, bool>(child_result, done));
        return;
      }

      long red_begin_tick = DateTime.UtcNow.Ticks;
      Hashtable new_result = new Hashtable();
      Hashtable my_entry = current_result as Hashtable;
      Hashtable value = child_result as Hashtable;

      if((my_entry == null) || (value == null)){
        if(my_entry != null){
          q.Enqueue(new Brunet.Collections.Pair<object, bool>(my_entry, done));
        }
        else if(value != null){
          q.Enqueue(new Brunet.Collections.Pair<object, bool>(value, done));
        }
        return;
      }

      new_result["count"] = (int) my_entry["count"] + (int) value["count"];
      new_result["consistency"] = (double) my_entry["consistency"] + (double) value["consistency"];
      int max_height = (int)my_entry["height"];
      int z = (int) value["height"] + 1;
      new_result["height"] = (z > max_height) ? z : max_height;

      reduce_time = DateTime.UtcNow.Ticks - red_begin_tick;
      OrderMapTime(ref new_result, my_entry, value);
//      OrderReduceTime(ref new_result, my_entry, value, reduce_time);  //Reduce time has negligible effect for completion time
      q.Enqueue(new Brunet.Collections.Pair<object, bool>(new_result, done));
    }

    private void HandleRpcException(ref bool another_done, double another_result,Channel ret, long begin_ticks){
      if(another_done == true){
        Hashtable my_entry = new Hashtable();
        my_entry["consistency"] = another_result / (2.0);
        my_entry["count"]=1;
        my_entry["height"] = 1;
        my_entry["map_time"] = (DateTime.UtcNow.Ticks - begin_ticks)/TimeSpan.TicksPerMillisecond;
        ret.Enqueue( my_entry );
      }
      else{
        another_done = true;
      }
    }

    private void OrderMapTime(ref Hashtable new_result, Hashtable current_value, Hashtable child_value){
      long map_time, max_map_time, min_map_time, total_map_time;

      max_map_time = (long)(current_value.ContainsKey("max_map_time") ? current_value["max_map_time"] : 0L);
      min_map_time = (long)(current_value.ContainsKey("min_map_time") ? current_value["min_map_time"] : 9223372036854775807L);
      total_map_time = (long)(current_value.ContainsKey("total_map_time") ? current_value["total_map_time"] : 0L);

      if(current_value.ContainsKey("map_time")){
        map_time = (long)current_value["map_time"];
        max_map_time = (map_time > max_map_time) ? map_time : max_map_time;
        min_map_time = (map_time < min_map_time) ? map_time : min_map_time;
        total_map_time += map_time;
      }

      if(child_value.ContainsKey("map_time")){
        map_time = (long)child_value["map_time"];
        max_map_time = (map_time > max_map_time) ? map_time : max_map_time;
        min_map_time = (map_time < min_map_time) ? map_time : min_map_time;
        total_map_time += map_time;
      }
      
      if(child_value.ContainsKey("max_map_time")){
        max_map_time = (max_map_time > (long)child_value["max_map_time"] ? max_map_time : (long)child_value["max_map_time"]);
      }
      if(child_value.ContainsKey("min_map_time")){
        min_map_time = (min_map_time < (long)child_value["min_map_time"] ? min_map_time : (long)child_value["min_map_time"]);
      }
      if(child_value.ContainsKey("total_map_time")){
        total_map_time += (long)child_value["total_map_time"];
      }
      
      new_result["max_map_time"] = max_map_time;
      new_result["min_map_time"] = min_map_time;
      new_result["total_map_time"] = total_map_time;
    }

    private void OrderReduceTime(ref Hashtable new_result, Hashtable current_value, Hashtable child_value, long reduce_time){
      long max_red_time, min_red_time, total_red_time;
      
      max_red_time = (long)(current_value.ContainsKey("max_red_time") ? current_value["max_red_time"] : 0L);
      min_red_time = (long)(current_value.ContainsKey("min_red_time") ? current_value["min_red_time"] : 9223372036854775807L);
      total_red_time = (long)(current_value.ContainsKey("total_red_time") ? current_value["total_red_time"] : 0L);

      if(child_value.ContainsKey("max_red_time")){
        max_red_time = (max_red_time > (long)child_value["max_red_time"] ? max_red_time : (long)child_value["max_red_time"]);
      }
      if(child_value.ContainsKey("min_red_time")){
        min_red_time = (min_red_time < (long)child_value["min_red_time"] ? min_red_time : (long)child_value["min_red_time"]);
      }
      if(child_value.ContainsKey("total_red_time")){
        total_red_time += (long)child_value["total_red_time"];
      }
      
      max_red_time = (reduce_time > max_red_time) ? reduce_time : max_red_time;
      min_red_time = (reduce_time < min_red_time) ? reduce_time : min_red_time;
      total_red_time += reduce_time;

      new_result["max_red_time"] = max_red_time;
      new_result["min_red_time"] = min_red_time;
      new_result["total_red_time"] = total_red_time;
    }
  }
}
