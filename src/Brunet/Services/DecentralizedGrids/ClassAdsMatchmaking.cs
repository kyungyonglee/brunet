using System;
using System.Collections;
using condor.classad;
using CondorSoap;

namespace Brunet.Services.DecenGrids{
  public enum ClassAdDataType{
    Xml,
    KeyValueString   // example "type"="Machine"
  }

  public class ClassAdObject{
    protected ClassAdParser _ca_parser;
    public readonly RecordExpr MatchingData;

    /*
        format: format of the data. possible value: ClassAdParser.XML or ClassAdParser.TEXT. XML=0 and TEXT=1
        ClassAdParser.TEXT not supported yet
    */
    public ClassAdObject(string data, int format){
      _ca_parser = new ClassAdParser(data ,format);
      MatchingData = (RecordExpr)_ca_parser.parse();
    }

    public ClassAdObject(string data) : this(data, ClassAdParser.XML){}

    public ClassAdObject(Hashtable ht){
      MatchingData = new RecordExpr();
      foreach(DictionaryEntry de in ht){
        try{
          ClassAdParser entry = new ClassAdParser(de.Value as string);
          Expr entry_expr = entry.parse();
//          Console.WriteLine("Decentralized Debug: {0} --> org:{1}\tClassAdParser:{2}", de.Key as string, de.Value as string, entry_expr);
          MatchingData.insertAttribute(de.Key as string, entry_expr);
        }catch{}
      }
    }

    public ClassAdObject(ClassAdStructAttr[] casa):this(CondorSoapInterface.ConvertToHashtable(casa)){}
  }
  
  public class ClassAdsMatchmaking{
    public ClassAdsMatchmaking(){}

    /*
            Return value: array of rank value. The first rank value = The first input argument's rank value.
            The second rank vlaue = The second input argument's rank value.
        */
    public static int[] CheckMatching(ClassAdObject query, ClassAdObject data){
      return ClassAd.match(query.MatchingData, data.MatchingData);
    }
  }
}

