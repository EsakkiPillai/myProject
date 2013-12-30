package com.ask.nrelate.rt.mr;

import com.ask.nrelate.lookup.Lookup;
import com.ask.nrelate.lookup.NRTLookup;
import com.ask.nrelate.rt.pojo.*;
import com.ask.nrelate.rt.utils.LogStatus;
import com.ask.nrelate.rt.utils.MapperCounter;
import com.ask.nrelate.rt.utils.RTLogType;
import com.ask.nrelate.rt.utils.RTMapperUtils;
import com.ask.nrelate.rt.utils.json.MarvelJson;
import com.ask.nrelate.utils.StringConstants;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.security.NoSuchAlgorithmException;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 15/10/13
 * Time: 1:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogNormalizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    //private Gson gson;
    private String geoDatabaseFile, fraudulentIPFile;
    private static LookupService geoLookupService = null;
    private static Lookup customLookupService = null;

    private Text mapKey = new Text(StringConstants.JSON_NORM_DEFAULT);
    private static final Logger LOG = Logger.getLogger(LogNormalizerMapper.class);
    private MultipleOutputs<Text, Text> mapperOutputService;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mapperOutputService = new MultipleOutputs<Text, Text>(context);
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        geoDatabaseFile = RTMapperUtils.getLocalCachedFile(paths, StringConstants.GEO_FILENAME);
        fraudulentIPFile= RTMapperUtils.getLocalCachedFile(paths, StringConstants.FRAUDULENT_FILENAME);

        try {
            geoLookupService = new LookupService(geoDatabaseFile, LookupService.GEOIP_MEMORY_CACHE);
            customLookupService = new NRTLookup(fraudulentIPFile);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text mapperKey = new Text(String.valueOf((int)(Math.random()*10000)));
        String mapperInput = value.toString();
        MapperOutput mapperOutput = new MapperOutput();
        String eventID = "", impLogDate = "";

        RTLogType rtLogType;
        try {
            // set the pixeldate either the log is valid or invalid
            Timestamp timestamp = new Gson().fromJson(value.toString(),Timestamp.class);
            mapperOutput.setLogDate(RTMapperUtils.generateHiveDate(timestamp.getRequestTS()));
            mapperOutput.setLogValue(value.toString());
            rtLogType = RTMapperUtils.getRTLogType(mapperInput);
        }catch (Exception e){
            mapperOutput.setLogValue(value.toString());
            mapperOutput.setErrorMessage(e.getMessage());
            mapperOutput.setLogStatus(LogStatus.INVALID.fieldName());
            context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
            return;
        }

        if(geoLookupService == null){
            try{
                geoLookupService = new LookupService(geoDatabaseFile, LookupService.GEOIP_MEMORY_CACHE);
            }catch(IOException e){
                LOG.error(e.getMessage());
            }
        }
        if(customLookupService == null){
            customLookupService = new NRTLookup(fraudulentIPFile);
        }

        if(rtLogType == null){
            mapperOutput.setLogValue(value.toString());
            mapperOutput.setErrorMessage(StringConstants.logRegexErrorMsg);
            mapperOutput.setLogStatus(LogStatus.INVALID.fieldName());
            context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
            return;
        }

        if(rtLogType.equals(RTLogType.IMPRESSION)){
            try {
                // parse impression elements
                Impression impression = MarvelJson.fromJson(mapperInput, Impression.class);

                //For generating the eventID, for uniqueness getting the counter.
                context.getCounter(MapperCounter.RECORD).increment(1L);
                eventID = RTMapperUtils.generateMD5(String.valueOf(context.getCounter(MapperCounter.RECORD).getValue() + impression.getRequestTS()));
                mapperOutput.setEventID(eventID);

                //Extracting the External content from internal data
                impression = RTMapperUtils.extractPartnerImpressions(impression);
                //Mapping impression object to the Mapper Output.
                mapperOutput = RTMapperUtils.convertImpressionToMapperOutput(impression);
                mapperOutput.setLogValue(value.toString());

                // generate platform and country
                mapperOutput = RTMapperUtils.generatePTC(geoLookupService, mapperOutput);

                //Filtering the Fraudulent Views and writing the records with status Fraudulent.
                if(customLookupService.isFraudulentIP(mapperOutput.getIpAddress(), mapperOutput.getUserAgent())){
                   mapperOutput.setLogStatus(LogStatus.FRAUDULENT.fieldName());
                }
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));

                /**
                 * Updating the Ad, Internal and External details and writing the output to the context.
                 */
                 impLogDate = mapperOutput.getLogDate();
                for(AdImpression adImpression : impression.getAd()){
                    /*mapperOutput = RTMapperUtils.convertAdImpressionToMapperOutput(adImpression, impression);
                    mapperOutput.setEventID(eventID);
                    mapperOutput.setLogDate(impLogDate);
                    context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));*/
                    AdImpressionOutput adImpressionOutput =   RTMapperUtils.convertAdImpressionToMapperOutput_v2(adImpression,impression);
                    adImpressionOutput.setLogDate(impLogDate);
                    adImpressionOutput.setLogValue(value.toString());
                    //Text text = RTMapperUtils.convertAdImpressionToMapperOutput(adImpressionOutput)       ;
                    mapperOutputService.write("cidImpressions",new Text(mapperKey), RTMapperUtils.convertAdImpressionToMapperOutput(adImpressionOutput));
                }
                /*for(InternalImpression internalImpression : impression.getInternal()){
                    mapperOutput = RTMapperUtils.convertIntImpressionToMapperOutput(internalImpression, impression);
                    mapperOutput.setEventID(eventID);
                    mapperOutput.setLogDate(impLogDate);
                    context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
                }*/


            } catch (Exception e) {
                mapperOutput.setLogValue(value.toString());
                mapperOutput.setErrorMessage(e.getMessage());
                mapperOutput.setLogStatus(LogStatus.INVALID.fieldName());
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
                return;
            }
        }
        else if(rtLogType.equals(RTLogType.AD)){
            try {
                Ad ad = MarvelJson.fromJson(mapperInput, Ad.class);
                mapperOutput = RTMapperUtils.convertAdToMapperOutput(ad);
                mapperOutput = RTMapperUtils.generatePTC(geoLookupService, mapperOutput);
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
            } catch (Exception e) {
                mapperOutput.setLogValue(value.toString());
                mapperOutput.setErrorMessage(e.getMessage());
                mapperOutput.setLogStatus(LogStatus.INVALID.fieldName());
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
                return;
            }

        }else if(rtLogType.equals(RTLogType.INT)){
            try {
                Internal internal = MarvelJson.fromJson(mapperInput, Internal.class);
                mapperOutput = RTMapperUtils.convertInternalToMapperOutput(internal);
                mapperOutput = RTMapperUtils.generatePTC(geoLookupService, mapperOutput);
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
            } catch (Exception e) {
                mapperOutput.setLogValue(value.toString());
                mapperOutput.setErrorMessage(e.getMessage());
                mapperOutput.setLogStatus(LogStatus.INVALID.fieldName());
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
                return;
            }
        } else if(rtLogType.equals(RTLogType.EXT)){
            try {
                External external = MarvelJson.fromJson(mapperInput, External.class);
                mapperOutput = RTMapperUtils.convertExternalToMapperOutput(external);
                mapperOutput = RTMapperUtils.generatePTC(geoLookupService, mapperOutput);
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
            } catch (Exception e) {
                mapperOutput.setLogValue(value.toString());
                mapperOutput.setErrorMessage(e.getMessage());
                mapperOutput.setLogStatus(LogStatus.INVALID.fieldName());
                context.write(mapperKey, new Text(RTMapperUtils.constructMapperOutput(mapperOutput)));
                return;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}