package com.ask.nrelate.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 23/10/13
 * Time: 12:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class IPFilter extends UDF {
    private static BufferedReader reader = null;
    private static String DELIMITER = ",";
    private static Set<String>  properties = new HashSet<String>();


    IPFilter(){
        try{
        reader = new BufferedReader(new FileReader("/ask/home/nrelate/nrReportingFraudulentIPs.csv"));
        String line="";
        while( (line = reader.readLine() )!= null ){
                properties.add(line);
            System.out.println(line);
        }
        }catch (Exception e){
            System.out.println("Input File doesnt exist");
        }
    }
    public String evaluate(String ip, String useragent) {

        String key = ip+"##"+useragent;
        if(properties.contains(key))
            return "FRAUDULENT";

        return "VALID";
    }

    public static void main(String[] args) throws FileNotFoundException,IOException {
        System.out.println(new IPFilter().evaluate("23.29.218.112","Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0; MAARJS)"));
    }
}
