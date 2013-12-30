package com.ask.nrelate.hive.udf;

import com.ask.nrelate.lookup.Lookup;
import com.ask.nrelate.lookup.RonLookup;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 3/10/13
 * Time: 3:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class AdOptionTagging extends UDF {
    private static Lookup lookup = new RonLookup("/ask/home/nrelate/nr_alldomains_ron","/ask/home/nrelate/nr_alldomains_ron_without_widgetid");
    public String evaluate(String pixeldate, String plugin, String domainid,String widgetid) {
                String widgetConfiguration = lookup.getWidgetConfiguration(pixeldate+plugin+domainid+widgetid);
        if(widgetConfiguration==null){
            widgetConfiguration = lookup.getWidgetConfigurationWithoutWidgetid(pixeldate+plugin+domainid);
        }
        String widgetElements[] = {"0","0","0"};
            if(widgetConfiguration!=null && widgetConfiguration.split("&").length>=3)
                widgetElements = widgetConfiguration.split("&");
        String noofads=widgetElements[1],noofrelatedposts=widgetElements[2],adopt=widgetElements[0];
        if(!isNumeric(noofads)){
            noofads = "0";
        }
        if(!isNumeric(noofrelatedposts)){
            noofrelatedposts = "0";
        }
        if(!isNumeric(adopt)){
            adopt = "0";
        }
        return adopt;
    }
    public boolean isNumeric(String value){
        if(value.matches("^\\-?\\d+$"))
            return true;
        else
            return false;
    }

    public static void main(String[] args){
        AdOptionTagging AdOptionTagging = new AdOptionTagging();
        System.out.println(AdOptionTagging.evaluate("2323", "US", "cbssports.com",""));
    }
}
