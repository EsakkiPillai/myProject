package com.ask.nrelate.hive.udf;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 12/7/13
 * Time: 3:28 PM
 * To change this template use File | Settings | File Templates.
 */
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.hive.ql.exec.UDF;

public class GeoIPLookupService extends UDF{
    private static String GEO_LOC = "/usr/local/share/GeoIP/GeoIP.dat";
    private static LookupService lookupService = null;
    GeoIPLookupService() throws Exception{
        lookupService = new LookupService(GEO_LOC,LookupService.GEOIP_MEMORY_CACHE);

    }
    public String evaluate(String ipAddress) throws Exception{
        return getCountry(ipAddress);
    }
    public String getCountry(String ipValue) throws Exception{
        if(lookupService==null)
            lookupService = new LookupService(GEO_LOC,LookupService.GEOIP_MEMORY_CACHE);
        String country = "";
        if(!ipValue.equals("-")){
            if(ipValue.contains(",")){
                String multipleIPList[] = ipValue.split(",");
                if (multipleIPList != null && multipleIPList.length > 0) {
                    String candidateIP = multipleIPList [0];
                    country = lookupService.getCountry(candidateIP).getCode();
                }
            }else {
                country = lookupService.getCountry(ipValue).getCode();
            }
        }
        return country;
    }
    public static void main(String args[])throws Exception{
        System.out.println(new GeoIPLookupService().evaluate("80-84-1-24"));
    }
}

