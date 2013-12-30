package com.ask.nrelate.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 15/10/13
 * Time: 12:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class FraudulentRank extends UDF{
    Map<String, Integer> rank = new HashMap<String, Integer>();

    public Integer evaluate(final String eventType) {
        if (rank.containsKey(eventType)){
            int rankTemp = rank.get(eventType);
            rank.put(eventType, new Integer(++rankTemp));
        } else{
            rank.put(eventType, new Integer(1));
        }
        return  rank.get(eventType);
    }
}
