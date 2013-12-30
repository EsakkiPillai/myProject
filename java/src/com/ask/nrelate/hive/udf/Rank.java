package com.ask.nrelate.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.util.Map;
import java.util.HashMap;

public final class Rank extends UDF {
    Map<String, Integer> rank = new HashMap<String, Integer>();
    //Integer rank = new Integer(0);
    public Integer evaluate(final String firstParam, final String secondParam) {
        if(firstParam == null || secondParam == null){return 0;}
        String domain = firstParam + secondParam;
//        if (domain == null) { return 0; }
        if (rank.containsKey(domain)){
            int rankTemp = rank.get(domain);
            rank.put(domain, new Integer(++rankTemp));
        }else{
            rank.put(domain, new Integer(1));
        }

        return rank.get(domain);
        //return ++ rank;
    }

    /*public Integer evaluate() {
        return ++ rank;
    }*/
}
