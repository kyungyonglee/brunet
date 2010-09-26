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
  public class MapReduceCheckDnsMatching: MapReduceBoundedBroadcast {
    public MapReduceCheckDnsMatching(Node n): base(n) {}
    public override void Map(Channel q, object map_arg) {
      Channel ret = new Channel(1);
      System.Random r = new Random((int)DateTime.UtcNow.Ticks);
      int ma = (int)map_arg;
      Console.WriteLine("map argument is " + ma);
      ma = r.Next(ma);
      Hashtable list = new Hashtable();
      list["count"]=1;
      if(ma == 0){
        byte[] result = new byte[200];
        list.Add("address", result);
      }
      
      q.Enqueue(list);

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
   
     Hashtable my_entry = current_result as Hashtable;
     Hashtable value = child_result as Hashtable;
     int count = (int) my_entry["count"];
     int y = (int) value["count"];
     my_entry["count"] = count + y;

     if(value.ContainsKey("address")){
        my_entry["address"] =  value["address"];            
     }
     q.Enqueue(new Brunet.Collections.Pair<object, bool>(my_entry, done));
    }
  }

  public class ZipfGenerator{
    private Random rnd = new Random((int)DateTime.UtcNow.Ticks);
    private int size;
    private double skew;
    private double bottom = 0;

    public ZipfGenerator(int size, double skew) {
      this.size = size;
      this.skew = skew;

      for(int i=1;i<size; i++) {
        this.bottom += (1/Math.Pow(i, this.skew));
      }
    }

    // the next() method returns an rank id. The frequency of returned rank ids are follows Zipf distribution.
    public int Next() {
      int rank;
      double friquency = 0;
      double dice;

      rank = rnd.Next(size);
      friquency = (1.0d / Math.Pow(rank, this.skew)) / this.bottom;
      dice = rnd.NextDouble();

      while(!(dice < friquency)) {
        rank = rnd.Next(size);
        friquency = (1.0d / Math.Pow(rank, this.skew)) / this.bottom;
        dice = rnd.NextDouble();
      }

      return rank;
    }

    // This method returns a probability that the given rank occurs.
    public double GetProbability(int rank) {
      return (1.0d / Math.Pow(rank, this.skew)) / this.bottom;
    }
  }
}

