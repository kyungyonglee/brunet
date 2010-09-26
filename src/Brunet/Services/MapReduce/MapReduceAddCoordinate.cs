/*
This program is part of BruNet, a library for the creation of efficient overlay
networks.
Copyright (C) 2008 Arijit Ganguly <aganguly@acis.ufl.edu> University of Florida  
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

using System;
using System.Collections;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Threading;

using Brunet.Concurrent;
using Brunet.Messaging;

namespace Brunet.Services.MapReduce {
  /**
   * This class implements a map-reduce task that allows counting number of 
   * nodes in a range and also depth of the resulting trees. 
   */ 
  public class MapReduceAddCoordinate: MapReduceBoundedBroadcast {
    public MapReduceAddCoordinate(Node n): base(n) {}
    public override void Map(Channel q, object map_arg) {
      IDictionary my_entry = new ListDictionary();
      my_entry["count"] = 1;
      Channel ret = new Channel(1);
      ret.CloseEvent += delegate(object o, EventArgs args) {
        RpcResult info_res = (RpcResult) ret.Dequeue();
        Hashtable ht = (Hashtable) info_res.Result;
        foreach(DictionaryEntry de in ht){
          if((string)de.Key == "geo_loc"){
            try{
              string location = de.Value as string;
              if(location != null){
                string[] sep = location.Split(',');
                double latitude = Double.Parse(sep[0]);
                double longitude =  Double.Parse(sep[1]);

                Process p = new Process();
                p.StartInfo.FileName = "condor_config_val";
                p.StartInfo.CreateNoWindow = true;
                p.StartInfo.Arguments = "-startd -rset \"COORDINATE = \\\"" + location + "\\\"\"";
                p.Start();
                p.WaitForExit();
                p.StartInfo.Arguments = "-startd -rset \"LATITUDE = " + latitude.ToString() + "\"";
                p.Start();
                p.WaitForExit();
                p.StartInfo.Arguments = "-startd -rset \"LONGITUDE = " + longitude.ToString() + "\"";
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
            }catch(Exception e){
              Console.WriteLine(e);
            }
          }
        }
      };
      _node.Rpc.Invoke(_node, ret, "Information.Info");
      q.Enqueue( my_entry );
    }
    
    public override void Reduce(Channel q, object reduce_arg, 
                                  object current_result, RpcResult child_rpc) {

      bool done = false;
      object child_result = null;

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
      
      IDictionary my_entry = current_result as IDictionary;
      IDictionary value = child_result as IDictionary;
      int count = (int) my_entry["count"];

      int y = (int) value["count"];
      my_entry["count"] = count + y;
      q.Enqueue(new Brunet.Collections.Pair<object, bool>(my_entry, done));
    }
  }
}
