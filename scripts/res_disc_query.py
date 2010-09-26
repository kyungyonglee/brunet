"""
Decentralized Matchmaking and Aggregation based Resource discovery : Query Script
usage: python res_disc_query.py --ip="ip address of target machine (e.g., 128.163.142.20 --> U of Kentucky machine)" --port=port number of xmlrpc server(e.g., 10000) --ma_req="requirement(mandatory)" --ma_rank="rank criteria(optinal)" --num_res=number of desired nodes to find in the query --sort_descending<if you want to order the rank value on the descending order. If you do not declare this, it will sort in the ascending order> --wf=type wait factor(optional value, default=0) --first_fit<if you want to use first fit method declare this option>
"""

import time, xmlrpclib, sys, getopt, base64, random, struct

def int_to_bytes(x, bytes):
  """Converts an integer to a msb first byte string of length bytes"""
  #I wish I could figure out a way to express the next four lines
  #a list comprehension, can you?
  bindata = []
  for i in xrange(bytes):
    (x, m) = divmod(x, 256)
    bindata.append(m)
  bindata.reverse()
  return "".join(map(chr, bindata))

def bytes_to_int(bindata):
  """Convert a sequence of bytes into a number"""
  return reduce(lambda x,y: (x<<8) | y, map(ord,bindata), 0)

class Address(object):
  def __init__(self, arg):
    try:
      self.num = long(arg)
      self.bindata = int_to_bytes(self.num, 20)
      self.str = 'brunet:node:' + base64.b32encode(self.bindata)
    except:
      s = str(arg)
      assert s.startswith('brunet:node:'), s
      self.bindata = base64.b32decode( s[12:44] )
      self.str = s[0:44]
      self.num = bytes_to_int(self.bindata)

  def __cmp__(self, other):
    if isinstance(other, Address):
      return cmp(self.num, other.num)
    else:
      return self.__cmp__(Address(other))

  def __long__(self):
    return self.num
  def __str__(self):
    return self.str

 
optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "ma_req=", "ma_rank=", "num_res=", "sort_ascending", "sort_descending", "period=", "wf=", "first_fit", "ip="])
map_arg={}
red_arg={}
wait_factor=0
for k,v in optlist:
  if k == "--port":
    port = int(v)
  elif k == "--ip":
    ip=str(v)
  elif k == "--ma_req":
    map_arg["requirements"]=str(v)
  elif k == "--ma_rank":
    map_arg["rank"]=str(v)
  elif k == "--num_res":
    red_arg["num_res"]=int(v)
  elif k == "--sort_descending":
    red_arg["sort_descending"]=1
  elif k == "--first_fit":
    red_arg["first_fit"]=1
  elif k == "--period":
    period = int(v)
  elif k == "--wf":
    wait_factor = int(v)
port=str(port)

rpc = xmlrpclib.Server("http://" + ip + ":" + port + "/xm.rem")
ht={}
neighbor_info = rpc.localproxy("sys:link.GetNeighbors")
self_addr=neighbor_info['self']
end_addr_num = Address(self_addr).num - 2L
end_addr = Address(end_addr_num).str
ht["map_arg"]=map_arg
ht["gen_arg"]=[self_addr,end_addr]
ht["reduce_arg"]=red_arg
ht["task_name"]="Brunet.Services.MapReduce.MapReduceDemoRf"
ht["wait_factor"]=wait_factor

def mr_start():
  begin_time = time.clock()
  result = rpc.localproxy("mapreduce.Start",ht)
  end_time = time.clock()
  print result
  print "total time taken = ", end_time-begin_time
  
mr_start()
