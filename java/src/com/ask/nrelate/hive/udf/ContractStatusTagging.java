package com.ask.nrelate.hive.udf;

import com.ask.nrelate.lookup.Lookup;
import com.ask.nrelate.lookup.NRTLookup;
import com.ask.nrelate.utils.StringConstants;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 11/7/13
 * Time: 11:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class ContractStatusTagging  extends UDF {
    private static Lookup customLookupService = new NRTLookup("/ask/home/nrelate/pagetype");
    public String evaluate(String pageTypeId, String country, String domain) {
        String pageTypeDerived = customLookupService.getPageType(pageTypeId + domain);
        if(pageTypeDerived == null){
            pageTypeDerived = customLookupService.getPageType(pageTypeId);
        }
        if(pageTypeDerived==null){
            pageTypeDerived = "non-article";
        }
        String contractStatus = "out-of-contract";
        if(pageTypeDerived != null && !domain.equals("und.com")){
            if(country.equalsIgnoreCase("US")){
                if(pageTypeDerived.toLowerCase().equalsIgnoreCase("article")){
                    contractStatus = "us-article";
                }else contractStatus = "in-contract";
            }else if(StringConstants.inContractCountries.contains(country)){
                contractStatus = "in-contract";
            }
        }
        return contractStatus;
    }
    
    public static void main(String[] args){
        ContractStatusTagging contractStatusTagging = new ContractStatusTagging();
        System.out.println(contractStatusTagging.evaluate("2323", "US", "cbssports.com"));
        System.out.println(contractStatusTagging.evaluate("6858", "US", "cbssports.com"));
        System.out.println(contractStatusTagging.evaluate("6858", "GB", "cbssports.com"));
        System.out.println(contractStatusTagging.evaluate("2101", "IN", "techrepublic.com"));
        System.out.println(contractStatusTagging.evaluate("2100", "US", "techrepublic.com"));
    }
}
