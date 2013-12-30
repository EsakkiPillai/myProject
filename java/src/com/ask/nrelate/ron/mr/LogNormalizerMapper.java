package com.ask.nrelate.ron.mr;

import com.ask.nrelate.lookup.Lookup;
import com.ask.nrelate.lookup.RonLookup;
import com.ask.nrelate.pojo.LogAttributes;
import com.ask.nrelate.pojo.MapperOutput;
import com.ask.nrelate.utils.StringConstants;
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import com.ask.nrelate.utils.MapperUtils;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * LogNormalizerMapper will parse the log and extract request information from the request URL and write it to the output collector
 * We are using older version of Map Reduce coding format, because Pentaho invoker doesn't have the support for newer one.
 */
public class LogNormalizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mapperOutputService;

    private String geoDatabaseFile, campaignRPC, ronCampaignRun,
            ronAllDomainsHistory, nrAllDomainsRon, nrAllDomainsRonWOWID, campaignPercent, fraudulentIPFile;

    List<String> logParameterList = null;

    private static LookupService geoLookupService = null;
    private static Lookup customLookupService = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mapperOutputService = new MultipleOutputs<Text, Text>(context);

        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        geoDatabaseFile = MapperUtils.getLocalCachedFile(paths, "GeoIP.dat");
        campaignRPC = MapperUtils.getLocalCachedFile(paths, "campaignRPC");
        ronCampaignRun = MapperUtils.getLocalCachedFile(paths, "ron_campaignRun");
        ronAllDomainsHistory = MapperUtils.getLocalCachedFile(paths, "ron_alldomains_history");
        nrAllDomainsRon = MapperUtils.getLocalCachedFile(paths, "nr_alldomains_ron");
        nrAllDomainsRonWOWID = MapperUtils.getLocalCachedFile(paths, "nr_alldomains_ron_without_widgetid");
        campaignPercent = MapperUtils.getLocalCachedFile(paths, "ron_campaignRPCPercent");
        fraudulentIPFile= MapperUtils.getLocalCachedFile(paths, "fraudulentIPFile");

        try {
            geoLookupService = new LookupService(geoDatabaseFile, LookupService.GEOIP_MEMORY_CACHE);

            customLookupService = new RonLookup(campaignRPC, ronCampaignRun, ronAllDomainsHistory,nrAllDomainsRon,
                    nrAllDomainsRonWOWID, campaignPercent, fraudulentIPFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logParameterList = new ArrayList<String>(){{
            add("remoteHost");
            add("identUser");
            add("authUser");
            add("date");
            add("time");
            add("timezone");
            add("method");
            add("url");
            add("protocol");
            add("status");
            add("bytes");
            add("referrer");
            add("userAgent");
            add("remoteIP");
            add("serverIP");
        }};
    }

    /**
     * Mapper module for Log Normalizer - RON
     */
    @Override
    public void map(LongWritable logKey, Text value, Context context) throws IOException, InterruptedException{
        Text mapperKey = new Text(String.valueOf(Math.random()*1000));
        MapperOutput mapperOutput = new MapperOutput();
        LogAttributes logAttributes;
        String logValue = value.toString();

        if(geoLookupService == null){
            try{
                geoLookupService = new LookupService(geoDatabaseFile, LookupService.GEOIP_MEMORY_CACHE);
            }catch(IOException e){
                e.printStackTrace();
            }
        }

        if(customLookupService == null){
            customLookupService = new RonLookup(campaignRPC, ronCampaignRun, ronAllDomainsHistory, nrAllDomainsRon,
                    nrAllDomainsRonWOWID, campaignPercent, fraudulentIPFile);
        }

        mapperOutput.setLogValue(logValue.replaceAll("\t", "|*|"));

        //Extracting the attributes from the input log
        logAttributes = MapperUtils.processLog(logValue, logParameterList);
        mapperOutput = MapperUtils.toMapperOutput(mapperOutput, logAttributes);

        //Extract the request timestamp from the log for placing the invalid log in the correct partitions.
        mapperOutput.setLogDate(MapperUtils.extractRequestDateFromLog(logValue, StringConstants.reqDatePattern));

        if(!logAttributes.isLogValid()){
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput, StringConstants.logRegexErrorMsg));
            return;
        }

        //Filtering the bot requested Log. Log Level Filtering
        mapperOutput = MapperUtils.filterBotLog(mapperOutput, logAttributes.getUserAgent(), logValue);
        if(!mapperOutput.isLogValid()){
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput));
            return;
        }

        try{
            mapperOutput.setLogDate(MapperUtils.generateHiveDate(logAttributes.getDate(), logAttributes.getTime()));
            //Adding Request time stamp for identifying suspect user clicks.
            mapperOutput.setRequestTS(MapperUtils.generateRequestTS(logAttributes.getDate(), logAttributes.getTime()));
        }catch(Exception e){
            e.printStackTrace();
        }

        //Extracting the plugin information and request type from the request URL.
        mapperOutput = MapperUtils.getPluginAndRequestType(mapperOutput, logAttributes);
        if(!mapperOutput.isLogValid()){
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput));
            return;
        }

        //Get the query parameter from the request URL.
        Map<String, String> urlMap = MapperUtils.getQueryParameters(logAttributes.getUrl());
        if(urlMap.containsKey("errorMessage")){
            mapperOutput.setLogStatus("Error");
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput, urlMap.get("errorMessage")));
            return;
        }

        mapperOutput.setDomain(MapperUtils.normalizeDomain(urlMap.get("domain")));

        if(mapperOutput.getRequestType().equals("impression") || mapperOutput.getRequestType().equals("realImpression") ||
                mapperOutput.getRequestType().equals("blockedImpression")){
            mapperOutput = MapperUtils.logImpressionAttributes(mapperOutput, urlMap);
        }else if(mapperOutput.getRequestType().equals("click")){
            mapperOutput = MapperUtils.logClickAttributes(mapperOutput, urlMap);
        }

        /**
         * Writing the invalid source url and its details to the output directory to the file prefixed with "invalidSourceURL"
         */
        if(null != mapperOutput.getOrgSourceURL() && !mapperOutput.getOrgSourceURL().isEmpty()){
            if(!MapperUtils.isValidSourceURL(mapperOutput.getOrgSourceURL())){
                mapperOutputService.write("invalidSourceURL",
                        new Text(mapperOutput.getOrgSourceURL()), MapperUtils.constructInvalidSourceURLOutput(mapperOutput));
            }
        }

        if(!mapperOutput.isLogValid()){
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput));
            return;
        }
        mapperOutput = MapperUtils.logGeneralAttributes(mapperOutput, urlMap);

        try{
            mapperOutput.setGeoCountry(MapperUtils.getCountry(geoLookupService, mapperOutput.getIpAddress()));
        }catch(Exception e){
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput, StringConstants.geoLookupExceptionMsg));
            return;
        }

        mapperOutput = MapperUtils.getValidDomain(mapperOutput);

        mapperOutput = MapperUtils.getDomainID(mapperOutput, customLookupService);
        if(mapperOutput.getRequestType().equals("click") && mapperOutput.getClickType().equals("ad")){
            mapperOutput = MapperUtils.getRate(mapperOutput, customLookupService);
        }

        if(mapperOutput.getRequestType().equals("impression") || mapperOutput.getRequestType().equals("realImpression")){
            mapperOutput = MapperUtils.getWidgetConfiguration(mapperOutput, customLookupService);
        }

        if(!MapperUtils.isNumeric(mapperOutput.getWidgetID())){
            mapperOutput.setWidgetID("0");
        }

        mapperOutput = MapperUtils.getValidData(mapperOutput);
        if(!mapperOutput.isLogValid()){
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput));
            return;
        }
        mapperOutput.setPlatform(MapperUtils.getPlatform(mapperOutput.getUserAgent()));

        mapperOutput.setLogStatus("VALID");
        if(mapperOutput.getRequestType().equals("blockedImpression")){
            mapperOutput.setLogStatus("BLOCKED");
        }

        if(mapperOutput.getRequestType().equals("impression")
                && customLookupService.isFraudulentIP(mapperOutput.getIpAddress(), mapperOutput.getUserAgent())){
            mapperOutput.setLogStatus("FRAUDULENT");
        }

        if(MapperUtils.isDemoEvent(mapperOutput.getReferrer())){
            mapperOutput.setLogStatus(StringConstants.LOG_STATUS_INVALID);
            mapperOutput.setErrorMessage(StringConstants.demoEventMsg);
            context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput));
            return;
        }

        if(StringConstants.ipInternal.contains(mapperOutput.getIpAddress())){
            mapperOutput.setLogStatus(StringConstants.LOG_STATUS_INVALID);
            mapperOutput.setErrorMessage(StringConstants.nRelateInternalIP);
        }
        mapperKey = new Text(mapperOutput.getIpAddress());

        context.write(mapperKey, MapperUtils.constructMapperOutput(mapperOutput, StringConstants.validLogMsg));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mapperOutputService.close();
    }
}