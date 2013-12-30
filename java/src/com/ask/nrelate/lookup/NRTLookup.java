package com.ask.nrelate.lookup;

import java.io.*;
import java.util.*;


public class NRTLookup implements Lookup{
    InputStream inputStream = null;
    Properties pagetypeProperties = null;
    Properties cpcProperties = null;
    Properties domainidProperties = null;
    Properties widgetProperties = null;
    List<String> fraudulentIPs = new ArrayList<String>();
    List<String> nonBillableURLPatterns = new ArrayList<String>();
    Map<String, String> pageTypes = new HashMap<String, String>(10);

    public NRTLookup(String pagetypeFile,String cpcFile, String domainidFile,
                     String widgetFile, String nonBillablePatternFile, String pageTypeOverrideFile, String fraudulentIPFile){
        pagetypeProperties = new Properties();
        cpcProperties = new Properties();
        domainidProperties = new Properties();
        widgetProperties = new Properties();
        BufferedReader bufferedReader = null;

        try {
            // Have to discuss about using the Hashmap for lookup
            inputStream = new FileInputStream(cpcFile);
            cpcProperties.load(inputStream);
            inputStream = new FileInputStream(domainidFile);
            domainidProperties.load(inputStream);
            inputStream = new FileInputStream(widgetFile);
            widgetProperties.load(inputStream);
            inputStream = new FileInputStream(pagetypeFile);
            pagetypeProperties.load(inputStream);
            inputStream = new FileInputStream(nonBillablePatternFile);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String currentLine;
            while((currentLine = bufferedReader.readLine()) != null){
                nonBillableURLPatterns.add(currentLine);
            }

            inputStream = new FileInputStream(pageTypeOverrideFile);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            while((currentLine = bufferedReader.readLine()) != null){
                pageTypes.put(currentLine.substring(0, currentLine.lastIndexOf("=")),
                        currentLine.substring(currentLine.lastIndexOf("=") + 1));
            }

            inputStream = new FileInputStream(fraudulentIPFile);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            while((currentLine = bufferedReader.readLine()) != null){
                fraudulentIPs.add(currentLine);
            }

        } catch (IOException e) {
            System.out.println("Error loading properties...");
        } finally{
            if(inputStream != null){
                try{
                    inputStream.close();
                }catch (Exception e){
                    System.out.println("Error loading properties...");
                }
            }
            if(bufferedReader != null){
                try{
                    bufferedReader.close();
                }catch (Exception e){
                    System.out.println("Error loading properties...");
                }
            }
        }
    }

    public NRTLookup(String fraudulentIPFile) {
        String currentLine = "";
        BufferedReader bufferedReader = null;
        try {
            inputStream = new FileInputStream(fraudulentIPFile);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            while((currentLine = bufferedReader.readLine()) != null){
                fraudulentIPs.add(currentLine);
            }
        } catch (IOException e) {
            
        } finally{
            if(inputStream != null){
                try{
                    inputStream.close();
                }catch (Exception e){
                    System.out.println("Error loading properties...");
                }
            }
            if(bufferedReader != null){
                try{
                    bufferedReader.close();
                }catch (Exception e){
                    System.out.println("Error loading properties...");
                }
            }
        }
        
    }

    /*public NRTLookup(String pageTypeOverrideFile){
        BufferedReader bufferedReader = null;
        try {
            inputStream = new FileInputStream(pageTypeOverrideFile);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String currentLine;
            while((currentLine = bufferedReader.readLine()) != null){
                pageTypes.put(currentLine.substring(0, currentLine.lastIndexOf("=")),
                        currentLine.substring(currentLine.lastIndexOf("=") + 1));
            }
        } catch (IOException e) {
            System.out.println("Error loading properties...");
        } finally {
            if(inputStream != null){
                try{
                    inputStream.close();
                }catch (Exception e){
                    System.out.println("Error loading properties...");
                }
            }
            if(bufferedReader != null){
                try{
                    bufferedReader.close();
                }catch (Exception e){
                    System.out.println("Error loading properties...");
                }
            }
        }
    }*/
/*
    public NRTLookup(String pageType){
        pagetypeProperties = new Properties();
        try{
            inputStream = new FileInputStream(pageType);
            pagetypeProperties.load(inputStream);
        }catch (IOException e) {
            System.out.println("Error loading properties...");
        } finally{
            if(inputStream != null){
                try{
                    inputStream.close();
                }catch (Exception e){
                    System.out.println("Error loading properties...");
                }
            }
        }
    }*/

    public String getPageType(String property){
        return pagetypeProperties.getProperty(property);
    }

    @Override
    public String getRPCPercent(String property) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getRPC(String property) {
        return null;
    }

    @Override
    public String getDomainid(String property){
        return domainidProperties.getProperty(property);
    }

    @Override
    public String getCPC(String property){
        return cpcProperties.getProperty(property);
    }

    @Override
    public String getWidgetConfiguration(String property){
        return widgetProperties.getProperty(property);
    }

    @Override
    public String getWidgetConfigurationWithoutWidgetid(String property) {
        return null;
    }

    public boolean isBillableURL(String pixelDate, String sourceURL){
        for(String nonBillableURLPattern : nonBillableURLPatterns){
            if(nonBillableURLPattern.startsWith(pixelDate) &&
                    sourceURL.contains(nonBillableURLPattern.substring(10)))
                return false;
        }
        return true;
    }

    public String getPageTypeOverride(String property){
        return pageTypes.get(property);
    }

    @Override
    public boolean isFraudulentIP(String ipAddress, String userAgent) {
        return fraudulentIPs.contains(ipAddress + userAgent);
    }

    public void close(){
        try{
            inputStream.close();
        } catch (IOException e) {
            System.out.println("Error while closing the inputstream");
        }
    }

    public static void main(String args[]){
        System.out.print(new NRTLookup("/home/kaniyarasu/Desktop/fraud").isFraudulentIP("66.249.93.90", "Mozilla/5.0 (en-US) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453 Safari/537.36 pss-webkit-request"));
        /*System.out.println(new NRTLookup("sample.txt").getPageType("time"));

        //Lookup lookup = new Lookup("pagetype","campaignRun","alldomains_history","nr_alldomains_nrt", "NRTNonBillableURLPatterns", "NRTContractStatusMapping");
        // Lookup lookup = new Lookup("/home/kaniyarasu/workspace/eureka/etl/lookup/nrt/source/com/nrelate/lookup/NRTContractStatusMapping");
        //System.out.print(lookup.isBillableURL("2013-05-22", "http://search.chow.com/search?query=Tara+Shioya&type=Story&sort_mode=newest"));
        // System.out.print(lookup.getPageTypeOverride("2013-05-22http://www.cbsnews.com/8301-18560_162-13502/show-schedule/"));
        String currentLine = "2013-05-22http://search.chow.com/search?query=amy+wisniewski&type=Story&sort_mode=newest=non-article";
        System.out.print(currentLine.substring(0, currentLine.lastIndexOf("=")) + currentLine.substring(currentLine.lastIndexOf("=")+1));*/
    }
}

