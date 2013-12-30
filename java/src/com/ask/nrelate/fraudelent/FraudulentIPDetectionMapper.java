package com.ask.nrelate.fraudelent;

import com.ask.nrelate.utils.MapperUtils;
import com.ask.nrelate.utils.StringConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: Kaniyarasu.D
 * Date: 24/7/13
 * Time: 12:01 PM
 */
public class FraudulentIPDetectionMapper extends Mapper<LongWritable, Text, FraudulentIPDetectionKey, Text> {
    private static String INPUT_DATA_REGEX = "^(\\S+)\\t([^\"]+)\\t(\\S+)\\t(\\S+)\\t(\\S+)\\t(\\S+)$";
    private List<String> logParameterList = null;
    private Map<String, String> inputData = null;
    private SimpleDateFormat hiveTimeFormat = new SimpleDateFormat("HH:mm:ss");

    @Override
    protected void 	setup(Context context) throws IOException, InterruptedException {
        logParameterList = new ArrayList<String>(){{
            add(StringConstants.IP_ADDRESS);
            add(StringConstants.USER_AGENT);
            add(StringConstants.DOMAIN);
            add(StringConstants.PLUGIN);
            add(StringConstants.REQUEST_TYPE);
            add(StringConstants.TIME);
        }};
    }

    public FraudulentIPDetectionMapper() {
        super();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputData = MapperUtils.extractDataFromLog(value.toString(), INPUT_DATA_REGEX, logParameterList);
        Text mapperOutput;
        FraudulentIPDetectionKey mapperKey = new FraudulentIPDetectionKey();
        int timeSlot = 1;
        long timeInMillis = 0L;
        Calendar calendar = GregorianCalendar.getInstance();

        if(inputData.size() > 0){
            try {
                calendar.setTime(hiveTimeFormat.parse(inputData.get(StringConstants.TIME)));
                timeSlot += calendar.get(Calendar.HOUR_OF_DAY) * 2 + calendar.get(Calendar.MINUTE) / 30;
                timeInMillis = Math.abs(calendar.getTimeInMillis())/1000;
            } catch (ParseException e) {

            }
            mapperKey.setIpAddress(inputData.get(StringConstants.IP_ADDRESS));
            mapperKey.setUserAgent(inputData.get(StringConstants.USER_AGENT));
            mapperKey.setDomain(inputData.get(StringConstants.DOMAIN));
            mapperKey.setRequestType(inputData.get(StringConstants.REQUEST_TYPE));

            mapperOutput = new Text(timeSlot
                    + StringConstants.tab + timeInMillis
                    + StringConstants.tab + inputData.get(StringConstants.PLUGIN));

            context.write(mapperKey, mapperOutput);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logParameterList = null;
        inputData = null;
    }
}
