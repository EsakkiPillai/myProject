package com.ask.nrelate.utils;

import au.com.bytecode.opencsv.CSVReader;
import com.ask.nrelate.lookup.Lookup;
import com.ask.nrelate.lookup.NRTLookup;
import com.ask.nrelate.lookup.RonLookup;
import com.ask.nrelate.pojo.LogAttributes;
import com.ask.nrelate.pojo.MapperOutput;
import com.maxmind.geoip.LookupService;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapperUtils {
    /**
     * Extract the data from given log based on the given regular expression.
     * @return List of parameters extracted from the given string.
     */
    public static Map<String, String> extractDataFromLog(String inputData, String regexForExtracting, List<String> parameterList){
        Map<String, String> parameters = new HashMap<String, String>();
        Pattern pattern = Pattern.compile(regexForExtracting);
        Matcher matcher = pattern.matcher(inputData);
        while(matcher.find()){
            for(int i=1; i <= matcher.groupCount(); i++) {
                parameters.put(parameterList.get(i - 1), matcher.group(i));
            }
        }

        return parameters;
    }

    /**
     * Returns the Map<String, String>. Parameters extracted from the log.
     */
    public static LogAttributes processLog(String logValue, List<String> logParameterList){
        Map<String,String> parameters;

        List<String> logPatterns = new ArrayList<String>(){{
            //Regex for tab separated log with server ip and remote ip
            add("^(\\S+(?:,\\s+\\S+)*)\\t(\\S+)\\t(\\S+)\\t\\[([^:]+):(\\d+:\\d+:\\d+) ([^\\]]+)\\]\\t\"(\\S+) (.+?) (\\S+)\"\\t(\\S+)\\t(\\S+)\\t\"([^\"]+)\"\\t\"([^\"]+)\"\t(\\S+)\t(\\S+)$");
            //Regex for tab separated log
            add("^(\\S+(?:,\\s+\\S+)*)\\t(\\S+)\\t(\\S+)\\t\\[([^:]+):(\\d+:\\d+:\\d+) ([^\\]]+)\\]\\t\"(\\S+) (.+?) (\\S+)\"\\t(\\S+)\\t(\\S+)\\t\"([^\"]+)\"\\t\"([^\"]+)\"");
            //Regex for space separated log(access log)
            add("^(\\S+(?:,\\s+\\S+)*) (\\S+) (\\S+) \\[([^:]+):(\\d+:\\d+:\\d+) ([^\\]]+)\\] \"(\\S+) (.+?) (\\S+)\" (\\S+) (\\S+) \"([^\"]+)\" \"([^\"]+)\"");

            add("^([^-]+)\\t(\\S+)\\t(\\S+)\\t\\[([^:]+):(\\d+:\\d+:\\d+) ([^\\]]+)\\]\\t\"(\\S+) (.+?) (\\S+)\"\\t(\\S+)\\t(\\S+)\\t\"([^\"]+)\"\\t\"([^\"]+)\"\\t(\\S+)\\t(\\S+)");

            add("^([^-]+)\\t(\\S+)\\t(\\S+)\\t\\[([^:]+):(\\d+:\\d+:\\d+) ([^\\]]+)\\]\\t\"(\\S+) (.+?) (\\S+)\"\\t(\\S+)\\t(\\S+)\\t\"([^\"]+)\"\\t\"([^\"]+)\"");
        }};

        for(String logPattern : logPatterns){
            parameters = extractDataFromLog(logValue, logPattern, logParameterList);
            if(parameters.size() > 0)
                return toLogAttributes(parameters);
        }

        return new LogAttributes(false);
    }

/*    *//**
     * Construct the mapper output by appending the extracted value(each separated by tab)
     * @return mapperOutput
     *//*
    public static Text constructMapperOutput(Map<String, String> outputParameters){
        String mapperOutput = null;
        for(String hiveAttr : hiveParameterList){
            if(mapperOutput == null){
                mapperOutput = outputParameters.get(hiveAttr);
                if(outputParameters.get("clickType").equals("ad")){
                    mapperOutput = outputParameters.get("requestTS") + "\t" + mapperOutput;
                }
            }
            else{
                mapperOutput = mapperOutput.concat("\t" + outputParameters.get(hiveAttr));
            }
        }
        return new Text(mapperOutput);
    }*/

    public static boolean isNumeric(String value){
        return value.matches("^\\-?\\d+$");
    }

    public static boolean isDouble(String value){
        return value.matches("^(\\d+)\\.(\\d+)$");
    }

    public static String getCountry(LookupService geoLookupService, String ipValue){
        String country = "NA";
        if(!ipValue.equals("-")){
            if(ipValue.startsWith(",")){
                ipValue = ipValue.substring(1).trim();
                country = geoLookupService.getCountry(ipValue).getCode();
            }else if(ipValue.contains(",")){
                String multipleIPList[] = ipValue.split(",");
                if (multipleIPList != null && multipleIPList.length > 0) {
                    String candidateIP = multipleIPList [0];
                    country = geoLookupService.getCountry(candidateIP).getCode();
                }
            }else {
                country = geoLookupService.getCountry(ipValue).getCode();
            }
        }
        return country;
    }

    public static String getDecodedURL(String url) {
        if(url != null){
            int position = url.indexOf("?");
            if(position!=-1){
                //url = url.substring(position);
                url = url.replace("?",StringConstants.QUESTION_MARK);
            }
            try{
                url = URLDecoder.decode(url, "UTF-8").replaceAll("\t", " ");
                //url = URLDecoder.decode(url, "UTF-8").replaceAll("\t", " ");

            }catch(Exception e){
                /*url = url.replaceAll("%u","%25u");
                try {
                    url = URLDecoder.decode(url, "UTF-8").replaceAll("\t", " ");
                    //url = URLDecoder.decode(url, "UTF-8").replaceAll("\t", " ");
                } catch (UnsupportedEncodingException e1) {

                }*/
            }
        }
        url = url.replace(StringConstants.QUESTION_MARK,"?");

        return url;
    }



    public static String getHost(String url){
        String domain = "NA";
        try{
            domain = new URL(url).getHost().trim();
        }catch(MalformedURLException e){
            e.printStackTrace();
        }
        return domain;
    }

    public static String normalizeDomain(String inputDomain){
        if(inputDomain != null){
            inputDomain = inputDomain.trim();
            return inputDomain.replaceAll("^www[0-9]*\\.","").toLowerCase();
        }
        return "NA";
    }

    /**
     * Return the local path of the Cached file based on the input filePattern
     */
    public static String getLocalCachedFile(Path[] paths, String filePattern){
        for(Path path : paths){
            if(path.toString().contains(filePattern))
                return path.toString();
        }
        return "";
    }

    /**
     * Construct the Mapper Output.
     */
    public static Text constructMapperOutput(MapperOutput mapperOutput, String message){
        mapperOutput.setErrorMessage(message);
        return constructMapperOutput(mapperOutput);
    }

    /*   *//**
     * Construct the Mapper Output.
     *//*
    public static Text constructMapperOutput(MapperOutput mapperOutput){

        String output = "\nipAddress : " + mapperOutput.getIpAddress() +
                "\nlogDate : " + mapperOutput.getLogDate() +
                "\ntimeZone : " + mapperOutput.getTimeZone() +
                "\nrequestType : " + mapperOutput.getRequestType() +
                "\nurl : " + mapperOutput.getRequestURL() +
                "\nclickType : " + mapperOutput.getClickType() +
                "\npluginType : " + mapperOutput.getPluginType() +
                "\nresponseCode : " + mapperOutput.getResponseCode() +
                "\nresponseSize : " + mapperOutput.getResponseSize() +
                "\nreferrer : " + mapperOutput.getReferrer() +
                "\nuserAgent : " + mapperOutput.getUserAgent() +
                "\ndecodedUrl : " + mapperOutput.getDecodedURL() +
                "\ndomain : " + mapperOutput.getDomain() +
                "\nspecificDomain : " + mapperOutput.getSpecificDomain() +
                "\nsourceUrl : " + mapperOutput.getSourceURL() +
                "\ndestinationUrl : " + mapperOutput.getDestinationURL() +
                "\ndestinationDomain : " + mapperOutput.getDestinationDomain() +
                "\ncid : " + mapperOutput.getCid() +
                "\ncpc : " + mapperOutput.getCpc() +
                "\nrpc : " + mapperOutput.getRpc() +
                "\nwidgetId : " + mapperOutput.getWidgetID() +
                "\npageType : " + mapperOutput.getPageType() +
                "\nreportGroup : " + mapperOutput.getReportGroup() +
                "\ngeoCountry : " + mapperOutput.getGeoCountry() +
                "\ngeoDefault : " + mapperOutput.getGeoDefault() +
                "\ngeoCity : " + mapperOutput.getGeoCity() +
                "\nuserId : " + mapperOutput.getUserID() +
                "\nsessionId : " + mapperOutput.getSessionID() +
                "\nrequestId : " + mapperOutput.getRequestID() +
                "\npageTypeId : " + mapperOutput.getPageTypeID() +
                "\narticleId : " + mapperOutput.getArticleID() +
                "\nkeywords : " + mapperOutput.getKeywords() +
                "\nresponseTime : " + mapperOutput.getResponseTime() +
                "\nisNewUser : " + mapperOutput.isNewUser() +
                "\nlogValue : " + mapperOutput.getLogValue() +
                "\nerrorMessage : " + mapperOutput.getErrorMessage() +
                "\nlogStatus : " + mapperOutput.getLogStatus() +
                "\ncontractStatus : " + mapperOutput.getContractStatus() +
                "\npageTypeDerived : " + mapperOutput.getPageTypeDerived() +
                "\ndomainId : " + mapperOutput.getDomainID() +
                "\nnoOfAds : " + mapperOutput.getNoOfAds() +
                "\nnoOfRelatedPosts : " + mapperOutput.getNoOfRelatedPosts() +
                "\nadOpt : " + mapperOutput.getAdOpt() +
                "\nadDomain : " + mapperOutput.getAdDomain() +
                "\nprId : " +  mapperOutput.getPrID() +
                "\ntopPosition : " + mapperOutput.getTopPosition() +
                "\nleftPosition : " + mapperOutput.getLeftPosition();

        return new Text(output);
    }*/

    /**
     * Construct the Mapper Output.
     */
    public static Text constructMapperOutput(MapperOutput mapperOutput){

        String output = mapperOutput.getIpAddress() +
                "\t" + mapperOutput.getLogDate() +
                "\t" + mapperOutput.getTimeZone() +
                "\t" + mapperOutput.getRequestType() +
                "\t" + mapperOutput.getRequestURL() +
                "\t" + mapperOutput.getClickType() +
                "\t" + mapperOutput.getPluginType() +
                "\t" + mapperOutput.getResponseCode() +
                "\t" + mapperOutput.getResponseSize() +
                "\t" + mapperOutput.getReferrer() +
                "\t" + mapperOutput.getUserAgent() +
                "\t" + mapperOutput.getDecodedURL() +
                "\t" + mapperOutput.getDomain() +
                "\t" + mapperOutput.getSpecificDomain() +
                "\t" + mapperOutput.getSourceURL() +
                "\t" + mapperOutput.getDestinationURL() +
                "\t" + mapperOutput.getDestinationDomain() +
                "\t" + mapperOutput.getCid() +
                "\t" + mapperOutput.getCpc() +
                "\t" + mapperOutput.getRpc() +
                "\t" + mapperOutput.getWidgetID() +
                "\t" + mapperOutput.getPageType() +
                "\t" + mapperOutput.getReportGroup() +
                "\t" + mapperOutput.getGeoCountry() +
                "\t" + mapperOutput.getGeoDefault() +
                "\t" + mapperOutput.getGeoCity() +
                "\t" + mapperOutput.getUserID() +
                "\t" + mapperOutput.getSessionID() +
                "\t" + mapperOutput.getRequestID() +
                "\t" + mapperOutput.getPageTypeID() +
                "\t" + mapperOutput.getArticleID() +
                "\t" + mapperOutput.getKeywords() +
                "\t" + mapperOutput.getResponseTime() +
                "\t" + mapperOutput.isNewUser() +
                "\t" + mapperOutput.getLogValue() +
                "\t" + mapperOutput.getErrorMessage() +
                "\t" + mapperOutput.getLogStatus() +
                "\t" + mapperOutput.getContractStatus() +
                "\t" + mapperOutput.getPageTypeDerived() +
                "\t" + mapperOutput.getDomainID() +
                "\t" + mapperOutput.getNoOfAds() +
                "\t" + mapperOutput.getNoOfRelatedPosts() +
                "\t" + mapperOutput.getAdOpt() +
                "\t" + mapperOutput.getAdDomain() +
                "\t" +  mapperOutput.getPrID() +
                "\t" + mapperOutput.getTopPosition() +
                "\t" + mapperOutput.getLeftPosition() + "\t0\t0\tNA" +
                "\t" + mapperOutput.isBillable()+
                "\t" + mapperOutput.getPlatform();


        /*if(mapperOutput.getClickType().equals("ad")){
            output = mapperOutput.getRequestTS() + "\t" + output;
        }*/

        return new Text(output);
    }

    /**
     * Extracts the request date from the log.
     */
    public static String extractRequestDateFromLog(String logValue, String reqDatePattern){
        Map<String, String> logParameters = MapperUtils.extractDataFromLog(logValue, reqDatePattern, new ArrayList<String>(){{
            add("date");
            add("time");
        }});
        return generateHiveDate(logParameters.get("date"), logParameters.get("time"));
    }

    /**
     * Format the date and time to the hive date format
     */
    public static String generateHiveDate(String date, String time){
        try {
            return  StringConstants.hiveDate.format(
                    StringConstants.logDateFormat.parse(
                            (date + StringConstants.colon
                                    + time).replaceAll("/", "-")
                    ));
        } catch (ParseException e) {
            return  StringConstants.hiveDate.format(new Date());
        }
    }

    /**
     * Format the date and time to the hive date format
     */
    public static long generateRequestTS(String date, String time){
        try {
            return  StringConstants.logDateFormat.parse(
                    (date + StringConstants.colon
                            + time).replaceAll("/", "-")
            ).getTime()/1000;
        } catch (ParseException e) {
            return new Date().getTime();
        }
    }

    /**
     * Converts the parsed output to LogAttributes Object
     */
    public static LogAttributes toLogAttributes(Map<String, String> logParameters){
        LogAttributes logAttributes = new LogAttributes(true);

        if((logParameters.get("remoteHost") == null || logParameters.get("remoteHost").trim().length() == 0
                || logParameters.get("remoteHost").equals("-")) && logParameters.get("remoteIP") != null) {
            logAttributes.setRemoteHost(logParameters.get("remoteIP"));
        }else{
            logAttributes.setRemoteHost(logParameters.get("remoteHost"));
        }
        logAttributes.setIdentUser(logParameters.get("identUser"));
        logAttributes.setAuthUser(logParameters.get("authUser"));
        logAttributes.setDate(logParameters.get("date"));
        logAttributes.setTime(logParameters.get("time"));
        logAttributes.setTimezone(logParameters.get("timezone"));
        logAttributes.setMethod(logParameters.get("method"));
        logAttributes.setUrl(logParameters.get("url"));
        logAttributes.setProtocol(logParameters.get("protocol"));
        logAttributes.setStatus(logParameters.get("status"));
        logAttributes.setBytes(logParameters.get("bytes"));
        logAttributes.setReferrer(logParameters.get("referrer"));
        logAttributes.setUserAgent(logParameters.get("userAgent"));
        logAttributes.setRemoteIP(logParameters.get("remoteIP"));
        logAttributes.setServerIP(logParameters.get("serverIP"));

        return logAttributes;
    }

    /**
     * Extracts the query parameters from request URL.
     */
    public static Map<String, String> getQueryParameters(String requestURL){
        Map<String, String> urlMap = new HashMap<String, String>();
        String decodedUrl = requestURL.substring(requestURL.indexOf("?")+1);
        decodedUrl = decodedUrl.replaceAll("%0d","%0A");
        CSVReader urlReader = new CSVReader(new StringReader(decodedUrl), '&');
        try{
            String[] queryParameters = urlReader.readAll().get(0);
            String quotes = "\\\\\"";
            for(String queryParameter : queryParameters){
                String list[] = queryParameter.split("=", 2);
                if(list.length>1){
                    try{
                        String tmpData = getDecodedURL(list[1]);
                        tmpData = tmpData.replaceAll("\"",quotes);
                        tmpData = tmpData.replaceAll("\n"," ").replaceAll("\t"," ").trim();
                        urlMap.put(list[0],tmpData);
                    }catch(Exception e){
                        urlMap.put("errorMessage", StringConstants.urlDecoderExceptionMsg);
                    }
                }else if(list.length == 1){
                    try{
                        String tmpData = "NA";
                        urlMap.put(list[0],tmpData);
                    }catch(Exception e){
                        urlMap.put("errorMessage", StringConstants.arrayIndexOutOfExceptionMsg);
                    }
                }

            }
        }
        catch(Exception e){
            urlMap.put("errorMessage", StringConstants.nullQueryParameterMsg);
        }
        return urlMap;
    }

    /**
     * Extract the plugin and request type from the request URL
     */
    public static MapperOutput getPluginAndRequestType(MapperOutput mapperOutput, LogAttributes logAttributes){
        String requestURL = logAttributes.getUrl();
        boolean validPlugin = true;
        if(requestURL.contains("/mpw_wp")){
            for(String mpBLKeyword : StringConstants.mpBLKeywords){
                if(requestURL.contains(mpBLKeyword)){
                    mapperOutput.setLogValid(false);
                    mapperOutput.setErrorMessage(StringConstants.mpBLMsg);
                    return mapperOutput;
                }
            }
            mapperOutput.setPluginType("mp");
        }else if(requestURL.contains("/rcw")){
            mapperOutput.setPluginType("rc");
        }else if(requestURL.contains("/fow_wp")){
            mapperOutput.setPluginType("fo");
        }else{
            validPlugin = false;
        }

        if(validPlugin){
            if(requestURL.contains("domain=") || requestURL.contains("track.html")){
                if(requestURL!=null){
                    if(requestURL.contains("track.html")){
                        mapperOutput.setRequestType("click");
                    }else{
                        mapperOutput.setRequestType("impression");
                    }
                }else{
                    mapperOutput.setLogValid(false);
                    mapperOutput.setErrorMessage(StringConstants.logWOUrlMsg);
                    return mapperOutput;
                }
            }else{
                mapperOutput.setLogValid(false);
                mapperOutput.setErrorMessage(StringConstants.logWODomainMsg);
                return mapperOutput;
            }
        }else if(requestURL.contains("/tracking/?")  || requestURL.contains("/r2/?") || requestURL.contains("/r3/?")){
            mapperOutput.setRequestType("click");
            if(requestURL.contains("plugin=")){
                try{
                    int position = requestURL.indexOf("plugin=");
                    mapperOutput.setPluginType(requestURL.substring(position+7,position+9));
                }catch(StringIndexOutOfBoundsException e){
                    mapperOutput.setLogValid(false);
                    mapperOutput.setErrorMessage(StringConstants.pbmRetrievingPluginMsg);
                    return mapperOutput;
                }
            }
        }else if(requestURL.contains("/vt/?")){
            mapperOutput.setRequestType("realImpression");
            if(requestURL.contains("plugin=")){
                try{
                    int position = requestURL.indexOf("plugin=");
                    mapperOutput.setPluginType(requestURL.substring(position+7,position+9));
                }catch(StringIndexOutOfBoundsException e){
                    mapperOutput.setLogValid(false);
                    mapperOutput.setErrorMessage(StringConstants.pbmRetrievingPluginMsg);
                    return mapperOutput;
                }
            }
        }else if(requestURL.contains("/common/report_blocked.php") && requestURL.contains("&all_partners_blocked=1")){
            mapperOutput.setRequestType("blockedImpression");
        }else {
            mapperOutput.setLogValid(false);
            mapperOutput.setErrorMessage(StringConstants.pluginNotFoundMsg);
            return mapperOutput;
        }

        if(!StringConstants.plugins.contains(mapperOutput.getPluginType().trim())){
           mapperOutput.setLogValid(false);
           mapperOutput.setErrorMessage(StringConstants.invalidPlugin);
           //return mapperOutput;
        }
        if(mapperOutput.getRequestType().equals("blockedImpression")){
            mapperOutput.setLogValid(true);
        }

        return mapperOutput;
    }

    /**
     * Filter the request from the bot
     */
    public static MapperOutput filterBotLog(MapperOutput mapperOutput, String userAgent, String logValue){
        logValue = logValue.toLowerCase();
        for(String botContent : StringConstants.botContents){
            if(logValue.contains(botContent)){
                mapperOutput.setLogValid(false);
                mapperOutput.setErrorMessage(StringConstants.logBotErrorMsg);
                return mapperOutput;
            }
        }

        userAgent = userAgent.toLowerCase();
        for(String badUser : StringConstants.badUsers){
            if(userAgent.contains(badUser)) {
                mapperOutput.setLogValid(false);
                mapperOutput.setErrorMessage(StringConstants.userAgentBLMsg);
                return mapperOutput;
            }
        }

        return mapperOutput;
    }

    public static MapperOutput logImpressionAttributes(MapperOutput mapperOutput, Map<String, String> urlMap){
        if(mapperOutput.getDomain() == null){
            mapperOutput.setLogValid(false);
            mapperOutput.setErrorMessage(StringConstants.noURLFoundErrorMsg);
            return mapperOutput;
        }
        mapperOutput.setDomain(normalizeDomain(getDecodedURL(mapperOutput.getDomain())));
        if(mapperOutput.getReferrer() != null){
            String ref = mapperOutput.getReferrer();
            if(ref.isEmpty() || ref.equals("-")){
                if(urlMap.get("url")==null){
                    mapperOutput.setReferrer(StringConstants.noRefFoundMsg);
                }else{
                    mapperOutput.setReferrer(urlMap.get("url"));
                }
            }
        }
        String srcUrl;
        if(urlMap.get("url")!=null && !urlMap.get("url").isEmpty()){
            srcUrl =  MapperUtils.getDecodedURL(urlMap.get("url"));
            mapperOutput.setOrgSourceURL(srcUrl.toLowerCase());
            if(srcUrl.length()>255)
                srcUrl = srcUrl.substring(0,255);
            srcUrl = srcUrl.replaceAll("\t", " ");
            mapperOutput.setSourceURL(srcUrl);
        }else{
            mapperOutput.setSourceURL("NA");
        }

        if(urlMap.containsKey("pr_id") && !urlMap.get("pr_id").isEmpty()){
            mapperOutput.setPrID(urlMap.get("pr_id"));
        }

        if(urlMap.containsKey("top") && !urlMap.get("top").isEmpty() && isNumeric(urlMap.get("top"))){
            try{
                mapperOutput.setTopPosition(Integer.parseInt(urlMap.get("top")));
            }catch (NumberFormatException e){
                mapperOutput.setTopPosition(0);
            }
        }

        if(urlMap.containsKey("left") && !urlMap.get("left").isEmpty() && isNumeric(urlMap.get("left"))){
            try{
                mapperOutput.setLeftPosition(Integer.parseInt(urlMap.get("left")));
            }catch (NumberFormatException e){
                mapperOutput.setLeftPosition(0);
            }
        }

        if(urlMap.containsKey("keywords") && !urlMap.get("keywords").isEmpty() ){
            mapperOutput.setKeywords(urlMap.get("keywords").replaceAll("\t", " "));
        }

        return mapperOutput;
    }

    public static MapperOutput logClickAttributes(MapperOutput mapperOutput, Map<String, String> urlMap){
        if(!urlMap.containsKey("src_url") ){
            mapperOutput.setLogValid(false);
            mapperOutput.setErrorMessage(StringConstants.noSourceURLFoundErrorMsg);
            return mapperOutput;
        }

        mapperOutput.setOrgSourceURL(urlMap.get("src_url").toLowerCase());
        String src_url=urlMap.get("src_url");
        mapperOutput.setSourceURL(src_url);

        if(MapperUtils.getDecodedURL(src_url).length()>255)
            mapperOutput.setSourceURL(MapperUtils.getDecodedURL(src_url).substring(0,255));
        if(mapperOutput.getDomain() == null || mapperOutput.getDomain().isEmpty()
                || mapperOutput.getDomain().equals("NA") || mapperOutput.getDomain().equals("cnet.com")){
            String sourceDomain = getHost(mapperOutput.getSourceURL());
            if(sourceDomain.equals("NA")){
                mapperOutput.setLogValid(false);
                mapperOutput.setErrorMessage(StringConstants.noSourceURLFoundErrorMsg);
                return mapperOutput;
            }
            mapperOutput.setDomain(normalizeDomain(sourceDomain));
        }

        if(urlMap.containsKey("type")&& !urlMap.get("type").isEmpty()){
            mapperOutput.setClickType(urlMap.get("type"));
        }else{
            mapperOutput.setClickType("internal");
        }
        if(!StringConstants.clicktypes.contains(mapperOutput.getClickType().trim())){
            if(mapperOutput.getClickType().trim().equals("rs")){
                mapperOutput.setClickType("ad");
                mapperOutput.setAdDomain("ask.com");
            }else{
                mapperOutput.setLogValid(false);
                mapperOutput.setErrorMessage(StringConstants.invalidClick);
                return mapperOutput;
            }
        }

        //Currently we are not using it, it is specific for Advertiser hgtv.com
        if(mapperOutput.getClickType().equals("avid")){
            mapperOutput.setDestinationURL("http://www.hgtv.com/video/design-star-6-is-coming-video/index.html");
            mapperOutput.setDestinationDomain(normalizeDomain(getHost(mapperOutput.getDestinationURL()).toLowerCase()));
        }else{
            if(urlMap.containsKey("dest_url")&& !urlMap.get("dest_url").isEmpty() ){
                mapperOutput.setDestinationURL(urlMap.get("dest_url"));
                if(urlMap.get("dest_url").length()>255)
                    mapperOutput.setDestinationURL(urlMap.get("dest_url").substring(0,255));
                if(mapperOutput.getDestinationURL().contains("http://")){
                    mapperOutput.setDestinationDomain(normalizeDomain(getHost(mapperOutput.getDestinationURL()).toLowerCase()));
                }else{
                    mapperOutput.setDestinationDomain(mapperOutput.getDomain());
                }
            }else{
                mapperOutput.setDestinationURL(mapperOutput.getSourceURL());
                mapperOutput.setDestinationDomain(mapperOutput.getDomain());
            }

            //Currently we are not using it, it is specific for Advertiser AARP.com
            if(mapperOutput.getDestinationDomain().equals("youtube.com")){
                for(String youtubeAttr : StringConstants.youtubeAttrs){
                    if(mapperOutput.getDestinationURL().contains(youtubeAttr))
                        mapperOutput.setDestinationDomain("rosettastone.com");
                }
            }else if(mapperOutput.getDestinationDomain().equals("aarp.org")
                    && mapperOutput.getDestinationURL().contains("aarp.org/espanol")){
                mapperOutput.setDestinationDomain("aarp.org/espanol");
            }
        }

        if(urlMap.containsKey("cid")&& !urlMap.get("cid").isEmpty() ){
            mapperOutput.setCid(urlMap.get("cid"));
        }

        return mapperOutput;
    }

    public static MapperOutput logGeneralAttributes(MapperOutput mapperOutput, Map<String, String> urlMap){
        if(urlMap.containsKey("plugin") && !urlMap.get("plugin").isEmpty() ){
            mapperOutput.setPluginType(urlMap.get("plugin"));
        }

        if(urlMap.containsKey("page_type")&& !urlMap.get("page_type").isEmpty() ){
            mapperOutput.setPageType(urlMap.get("page_type"));
        }

        if(urlMap.containsKey("page_type_id") && !urlMap.get("page_type_id").isEmpty() && isNumeric(urlMap.get("page_type_id"))){
            mapperOutput.setPageTypeID(urlMap.get("page_type_id"));
        }

        if(urlMap.containsKey("widget_id") && !urlMap.get("widget_id").isEmpty() && isNumeric(urlMap.get("widget_id"))){
            mapperOutput.setWidgetID(urlMap.get("widget_id"));
        }

        if(urlMap.containsKey("article_id")&& !urlMap.get("article_id").isEmpty() ){
            mapperOutput.setArticleID(urlMap.get("article_id"));
        }

        if(urlMap.containsKey("geo")&& !urlMap.get("geo").isEmpty() ){
            mapperOutput.setGeoDefault(urlMap.get("geo"));
        }

        if((!mapperOutput.getSourceURL().contains(mapperOutput.getDomain()) || mapperOutput.getSourceURL().isEmpty()
                || mapperOutput.getSourceURL().equals("NA") || StringConstants.invalidSourceURLPattern.contains(mapperOutput.getSourceURL()))
                && !mapperOutput.getReferrer().equals("-")){
            mapperOutput.setSourceURL(mapperOutput.getReferrer());
        }


        if(mapperOutput.getSourceURL().isEmpty() || mapperOutput.getSourceURL().equals("NA")
                || StringConstants.invalidSourceURLPattern.contains(mapperOutput.getSourceURL())){
                if(urlMap.get("referrer") != null)
                    mapperOutput.setSourceURL(urlMap.get("referrer"));
        }

        return mapperOutput;
    }

    public static MapperOutput getValidDomain(MapperOutput mapperOutput){
        String domain = mapperOutput.getDomain();

        if(domain.contains("%")){
            try{
                domain = URLDecoder.decode(domain,"UTF-8");
                if(domain.contains("http://")){
                    domain = getHost(domain);
                }
            }catch(Exception e){
                mapperOutput.setLogStatus("Error");
                mapperOutput.setErrorMessage(StringConstants.urlDecoderExceptionMsg);
            }
        }

        String srcUrl = mapperOutput.getSourceURL();
        if(srcUrl.contains("%")){
            try{
                mapperOutput.setSourceURL(URLDecoder.decode(srcUrl,"UTF-8").replaceAll("\t", " ")
                        .replaceAll("\n", " ").trim());
            }catch(Exception e){
                mapperOutput.setLogStatus("Error");
                mapperOutput.setErrorMessage(StringConstants.urlDecoderExceptionMsg);
            }
        }

        domain = normalizeDomain(domain).replaceAll("http://","");
        if(!domain.contains("localhost")){
            if(!domain.contains(".")){
                mapperOutput.setLogValid(false);
                mapperOutput.setErrorMessage(StringConstants.invalidDomain);
                return mapperOutput;
            }
        }

        while(domain.endsWith(".")){
            domain = domain.substring(0,domain.length() - 1);
        }

        if(domain.contains("cnet.com")){
            mapperOutput.setSpecificDomain("cnet.com");
        }else{
            mapperOutput.setSpecificDomain(domain);
        }

        if(domain.contains(".blogspot.")){
            String specific_domain=domain;
            domain = domain.substring(0,domain.indexOf(".blogspot"))+".blogspot.com";
            mapperOutput.setSpecificDomain(specific_domain);
            mapperOutput.setDomain(domain);
        }

        return mapperOutput;
    }

    public static MapperOutput toMapperOutput(MapperOutput mapperOutput, LogAttributes logAttributes){
        if(logAttributes.getRemoteHost() != null && logAttributes.getRemoteHost().startsWith(",")){
            mapperOutput.setIpAddress(logAttributes.getRemoteHost().substring(1).trim());
        }else {
            mapperOutput.setIpAddress(logAttributes.getRemoteHost());
        }
        mapperOutput.setTimeZone(logAttributes.getTime());
        mapperOutput.setRequestURL(logAttributes.getMethod() + " "
                + logAttributes.getUrl() + " "
                + logAttributes.getProtocol());
        mapperOutput.setResponseSize(logAttributes.getBytes());
        mapperOutput.setResponseCode(logAttributes.getStatus());
        mapperOutput.setReferrer(logAttributes.getReferrer());
        mapperOutput.setUserAgent(logAttributes.getUserAgent());
       // mapperOutput.setServerIP(logAttributes.getServerIP());
        return mapperOutput;
    }

    public static MapperOutput getDomainID(MapperOutput mapperOutput, Lookup customLookupService){
        String pixelDate = mapperOutput.getLogDate().split(" ")[0];

        String id = customLookupService.getDomainid(pixelDate+mapperOutput.getDomain());
        if(mapperOutput.getDomain().contains(".cnet.com")){
            id = customLookupService.getDomainid(pixelDate+"cnet.com");
        }
        if(id==null || !isNumeric(id)){
            id = "0";
        }
        mapperOutput.setDomainID(Integer.parseInt(id));

        return mapperOutput;
    }

    public static MapperOutput getRate(MapperOutput mapperOutput, Lookup customLookupService){

        String pixelDate = mapperOutput.getLogDate().split(" ")[0];

        if(!isNumeric(mapperOutput.getCid())){
            mapperOutput.setCid("0");
        }

        String cpcWithDomain = customLookupService.getCPC(pixelDate + mapperOutput.getCid());
        //String cpcWithDomain = " "+lookup.cpcProperties.size();
        String cpc[] = {"0","NA","0"};
        String rpc ="0";
        if(customLookupService instanceof RonLookup){
            if(cpcWithDomain != null && cpcWithDomain.split("&").length == 3){
                cpc = cpcWithDomain.split("&");
            }
            rpc = cpc[2];
            // rpc calculation
            String rpcPercent = customLookupService.getRPCPercent(pixelDate + mapperOutput.getDomainID());
            //String rpcPercent = lookup.getRPCPercent("2013-04-015000007");
            if(rpcPercent != null){
                double rpcPercentValue = Double.parseDouble(rpcPercent);
                rpcPercentValue = rpcPercentValue/100;
                double cpcVal = Double.parseDouble(cpc[0]);
                rpc = String.valueOf(cpcVal*rpcPercentValue);
            } else{
                String rpcValue = customLookupService.getRPC(pixelDate+ mapperOutput.getDomainID()
                        + mapperOutput.getCid());
                if(rpcValue!=null){
                    rpc = rpcValue;
                }
            }
            if(isDouble(cpc[0])){
                mapperOutput.setCpc(Double.parseDouble(cpc[0]));
            }

            if(isDouble(rpc)){
                mapperOutput.setRpc(Double.parseDouble(rpc));
            }
            mapperOutput.setAdDomain(cpc[1]);
        } else{
            if(cpcWithDomain != null && cpcWithDomain.split("&").length >= 2){
                cpc = cpcWithDomain.split("&");
            }

            if(isDouble(cpc[0])){
                mapperOutput.setCpc(Double.parseDouble(cpc[0]));
                mapperOutput.setRpc(Double.parseDouble(cpc[0]));
            }
            mapperOutput.setAdDomain(cpc[1]);
        }


        return mapperOutput;
    }

    public static MapperOutput getWidgetConfiguration(MapperOutput mapperOutput, Lookup customLookupService){

        String pixelDate = mapperOutput.getLogDate().split(" ")[0];
        String widgetConfiguration = "";
        if(customLookupService instanceof RonLookup){
            widgetConfiguration = customLookupService.getWidgetConfiguration(pixelDate
                    + mapperOutput.getPluginType() + mapperOutput.getDomainID() + mapperOutput.getWidgetID());
            if(widgetConfiguration == null){
                widgetConfiguration = customLookupService.getWidgetConfigurationWithoutWidgetid(pixelDate
                        + mapperOutput.getPluginType() + mapperOutput.getDomainID());
            }
        }else if(customLookupService instanceof NRTLookup){
            widgetConfiguration = customLookupService.getWidgetConfiguration(pixelDate
                    + mapperOutput.getPluginType() + mapperOutput.getDomainID() + mapperOutput.getWidgetID());
/*            String tmpWidgetConfiguration = "NA";
            if(mapperOutput.getDomain().contains(".cnet.com")){
                tmpWidgetConfiguration = customLookupService.getWidgetConfiguration(pixelDate
                        + mapperOutput.getPluginType() + "cnet.com"+ mapperOutput.getWidgetID());
            }
            if(widgetConfiguration==null){
                if(tmpWidgetConfiguration!=null)
                    widgetConfiguration = tmpWidgetConfiguration;
            }*/
        }

        String widgetElements[] = {"0","0","0"};
        if(widgetConfiguration!=null && widgetConfiguration.split("&").length>=3)
            widgetElements = widgetConfiguration.split("&");
        if(isNumeric(widgetElements[0])){
            mapperOutput.setNoOfAds(Integer.parseInt(widgetElements[0]));
        }
        if(isNumeric(widgetElements[1])){
            mapperOutput.setNoOfRelatedPosts(Integer.parseInt(widgetElements[1]));
        }
        if(isNumeric(widgetElements[2])){
            mapperOutput.setAdOpt(Integer.parseInt(widgetElements[2]));
        }

        return mapperOutput;
    }

    public static MapperOutput getContractStatus(MapperOutput mapperOutput, Lookup customLookupService){
        String pixelDate = mapperOutput.getLogDate().split(" ")[0];
        String pageTypeDerived =  customLookupService.getPageTypeOverride(pixelDate +
                mapperOutput.getSourceURL());

        if(pageTypeDerived == null){
            pageTypeDerived = customLookupService.getPageType(mapperOutput.getPageTypeID() + mapperOutput.getDomain());
        }
        if(pageTypeDerived == null){
            pageTypeDerived = customLookupService.getPageType(mapperOutput.getPageTypeID());
        }
        if(pageTypeDerived==null){
            pageTypeDerived = "non-article";
        }
        mapperOutput.setPageTypeDerived(pageTypeDerived);
        String contractStatus = "out-of-contract";
        if(pageTypeDerived != null && !mapperOutput.getDomain().equals("und.com")){
            if(mapperOutput.getGeoCountry().equalsIgnoreCase("US")){
                if(pageTypeDerived.toLowerCase().equalsIgnoreCase("article")){
                    contractStatus = "us-article";
                }else contractStatus = "in-contract";
            }else if(StringConstants.inContractCountries.contains(mapperOutput.getGeoCountry())){
                contractStatus = "in-contract";
            }
        }
        mapperOutput.setContractStatus(contractStatus);

        return mapperOutput;
    }

    public static MapperOutput getBillable(MapperOutput mapperOutput, Lookup customLookupService){
        String pixelDate = mapperOutput.getLogDate().split(" ")[0];
        mapperOutput.setBillable(customLookupService.isBillableURL(pixelDate, mapperOutput.getSourceURL()));
        return mapperOutput;
    }

    public static MapperOutput getValidData(MapperOutput mapperOutput){
        List<String> plugins = new ArrayList<String>(){{
            add("rc");
            add("mp");
            add("fo");
            add("NA");
        }};
        List<String> clickTypes =new ArrayList<String>(){{
            add("ad");
            add("external");
            add("internal");
            add("NA");
        }};

        for(Field f : mapperOutput.getClass().getDeclaredFields()){
            if(f.getType().getSimpleName().equals("String")
                    && !StringConstants.blackListVarForValidation.contains(f.getName().toLowerCase())){
                try {
                    String value = PropertyUtils.getProperty(mapperOutput, f.getName()).toString();
                    PropertyUtils.setProperty(mapperOutput, f.getName(), getValidData(value));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
            }
        }

        if(!clickTypes.contains(mapperOutput.getClickType()) || !plugins.contains(mapperOutput.getPluginType())){
            if(mapperOutput.getClickType().trim().equals("rs")){
                mapperOutput.setClickType("ad");
                mapperOutput.setAdDomain("ask.com");
                return mapperOutput;
            }
            mapperOutput.setLogValid(false);
            mapperOutput.setErrorMessage(StringConstants.invalidPluginOrClick);
            return mapperOutput;
        }
        return mapperOutput;
    }

    public static String getValidData(String content){
        if(content != null){
            if(content.isEmpty()){
                return "NA";
            }else if(content.length() > 255){
                return content.substring(0,255);
            }
        }
        return content;
    }

    public static String getPlatform(String userAgent) {
        userAgent = userAgent.toLowerCase();

        if((userAgent.length() > 4) && (userAgent.matches("(?i).*((android|bb\\d+|meego).+mobile|android|mobi|avantgo|bada\\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od|ad)|iris|kindle|lge |maemo|midp|mmp|htc|tablet|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\\.(browser|link)|vodafone|wap|windows (ce|phone)|xda|xiino).*")||
                userAgent.substring(0,4).matches("(?i)1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\\-(n|u)|c55\\/|capi|ccwa|cdm\\-|cell|chtm|cldc|cmd\\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\\-s|devi|dica|dmob|do(c|p)o|ds(12|\\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\\-|_)|g1 u|g560|gene|gf\\-5|g\\-mo|go(\\.w|od)|gr(ad|un)|haie|hcit|hd\\-(m|p|t)|hei\\-|hi(pt|ta)|hp( i|ip)|hs\\-c|ht(c(\\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\\-(20|go|ma)|i230|iac( |\\-|\\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\\/)|klon|kpt |kwc\\-|kyo(c|k)|le(no|xi)|lg( g|\\/(k|l|u)|50|54|\\-[a-w])|libw|lynx|m1\\-w|m3ga|m50\\/|ma(te|ui|xo)|mc(01|21|ca)|m\\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\\-2|po(ck|rt|se)|prox|psio|pt\\-g|qa\\-a|qc(07|12|21|32|60|\\-[2-7]|i\\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\\-|oo|p\\-)|sdk\\/|se(c(\\-|0|1)|47|mc|nd|ri)|sgh\\-|shar|sie(\\-|m)|sk\\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\\-|v\\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\\-|tdg\\-|tel(i|m)|tim\\-|t\\-mo|to(pl|sh)|ts(70|m\\-|m3|m5)|tx\\-9|up(\\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\\-|your|zeto|zte\\-"))) {
            if(userAgent.indexOf("android") != -1 && (userAgent.indexOf("mobile") == -1 || userAgent.indexOf("tablet") != -1)){
                return DeviceType.Tablet.toString();
            }

            if(userAgent.indexOf("ipad") != -1 || userAgent.indexOf("tablet") != -1){
                return DeviceType.Tablet.toString();
            }

            return DeviceType.Mobile.toString();
        }
        return DeviceType.Desktop.toString();
    }

    /**
     * Extracts invalid source url patterns from processing data.
     * @param sourceURL
     * @return
     */
    public static boolean isValidSourceURL(String sourceURL){
        /*//String urlRegex = "^((https?)://((www|([a-z0-9\\-]+)+)\\.){0,2})?[a-z0-9\\-]+(\\.[a-z0-9\\-]+)+([/?].*)?$";
        String urlRegex = "^((https?)://)?((www|([a-z0-9\\-]+)+)\\.){1,3}[a-z0-9\\-]+.*";
        Pattern pattern = Pattern.compile(urlRegex);
        Matcher matcher = pattern.matcher(sourceURL);
        return matcher.matches();
        //return false;*/
        String[] schemes = {"http","https"};
        UrlValidator urlValidator = new UrlValidator(schemes);
        return urlValidator.isValid(sourceURL);
    }

    /**
     * Constructs the output for the invalid source url processing - field tab separated. 
     * @return
     */
    public static Text constructInvalidSourceURLOutput(MapperOutput mapperOutput){
        return new Text(mapperOutput.getLogValue() + StringConstants.tab
                + mapperOutput.getRequestType() + StringConstants.tab
                + mapperOutput.getClickType() + StringConstants.tab
                + mapperOutput.getLogDate().split(" ")[0]
                + mapperOutput.getDomain()
        );
    }
    
    public static boolean isDemoEvent(String referrer){
        return referrer.startsWith(StringConstants.DEMO_REF);
    }

    public static void main(String[] args){
        System.out.println(isDemoEvent("http://demo.nrelate.com/247wallst/247wallst-demo-ab.html"));
        System.out.println(isDemoEvent("http://demo1.nrelate.com/247wallst/247wallst-demo-ab.html"));
    }
}
