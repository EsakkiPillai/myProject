package com.ask.nrelate.lookup;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class RonLookup implements  Lookup{
    InputStream inputStream = null;
    public Properties rpcOverrideProperties = null;
    public Properties cpcProperties = null;
    public Properties domainidProperties = null;
    public Properties widgetProperties = null;
    public Properties widgetProperties2 = null;
    public Properties rpcPercentProperties = null;
    List<String> fraudulentIPs = new ArrayList<String>();

    @Override
    public String getPageType(String property) {
        return null;
    }

    public RonLookup(String rpcOverrideFile,String cpcFile,String domainidFile,
                     String widgetFile,String widgetFile2, String rpcPercentFile, String fraudulentIPFile){
        rpcOverrideProperties = new Properties();
        cpcProperties = new Properties();
        domainidProperties = new Properties();
        widgetProperties = new Properties();
        widgetProperties2 = new Properties();
        rpcPercentProperties = new Properties();

        BufferedReader bufferedReader = null;

        //String testPath = "/home/kaniyarasu/workspace/eureka/java/src/com/ask/nrelate/lookup/";
        try {
            // Have to discuss about using the Hashmap for RonLookup
            inputStream = new FileInputStream(cpcFile);
            cpcProperties.load(inputStream);
            inputStream = new FileInputStream(domainidFile);
            domainidProperties.load(inputStream);
            inputStream = new FileInputStream(widgetFile);
            widgetProperties.load(inputStream);
            inputStream = new FileInputStream(widgetFile2);
            widgetProperties2.load(inputStream);
            inputStream = new FileInputStream(rpcOverrideFile);
            rpcOverrideProperties.load(inputStream);
            inputStream = new FileInputStream(rpcPercentFile);
            rpcPercentProperties.load(inputStream);
            //inputStream.close();
            inputStream = new FileInputStream(fraudulentIPFile);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String currentLine;
            while((currentLine = bufferedReader.readLine()) != null){
                fraudulentIPs.add(currentLine.trim());
            }
        } catch (IOException e) {
            System.out.println("Error loading properties...");
        }
    }

    public RonLookup(String widgetFile,String widgetFileWithoutWidgetid){
        widgetProperties = new Properties();
        widgetProperties2 = new Properties();

        //String testPath = "/home/kaniyarasu/workspace/eureka/java/src/com/ask/nrelate/lookup/";
        try {
            // Have to discuss about using the Hashmap for RonLookup
            inputStream = new FileInputStream(widgetFile);
            widgetProperties.load(inputStream);
            inputStream = new FileInputStream(widgetFileWithoutWidgetid);
            widgetProperties2.load(inputStream);
            //inputStream.close();

        } catch (IOException e) {
            System.out.println("Error loading properties...");
        }
    }
    public RonLookup(String rpcOverrideFile){
        cpcProperties = new Properties();
        try {
            inputStream = getClass().getResourceAsStream(rpcOverrideFile);
            // Have to discuss about using the Hashmap for RonLookup
            cpcProperties.load(inputStream);

        } catch (IOException e) {
            System.out.println("Error loading properties...");
        }
    }

    public String getRPC(String property){
        return rpcOverrideProperties.getProperty(property);
    }
    public String getDomainid(String property){
        return domainidProperties.getProperty(property);
    }
    public String getCPC(String property){
        return cpcProperties.getProperty(property);
    }
    public String getWidgetConfiguration(String property){
        return widgetProperties.getProperty(property);
    }
    public String getWidgetConfigurationWithoutWidgetid(String property){
        return widgetProperties2.getProperty(property);
    }
    public String getRPCPercent(String property){
        return rpcPercentProperties.getProperty(property);
    }

    @Override
    public boolean isFraudulentIP(String ipAddress, String userAgent) {
        return fraudulentIPs.contains(ipAddress + userAgent);
    }

    @Override
    public boolean isBillableURL(String pixelDate, String sourceURL) {
        return false;
    }

    @Override
    public String getPageTypeOverride(String property) {
        return null;
    }

    public static void main(String args[]){
        System.out.println(new RonLookup("campaignRun").getCPC("2013-01-0210714"));
    }
}
