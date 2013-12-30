package com.ask.nrelate.nrt.mr;

import com.ask.nrelate.utils.ReducerUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LogNormalizerReducer extends Reducer<Text, Text, Text,Text> {
    Map<String, String> clickFrequency;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        clickFrequency = ReducerUtils.loadClickFrequency(DistributedCache.getLocalCacheFiles(context.getConfiguration()));
    }

    /**
     * Reduce emits just emits the log with status(Duplicate/Valid).
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<String>();
        Long previousDate, currentDate = 0L;
        String reducerValue;

        if(key.toString().endsWith("type:ad")){
            for(Text value : values){
                list.add(value.toString());
            }
            Collections.sort(list);
            String lookupValue = clickFrequency.get(key.toString());
            if(lookupValue != null){
                previousDate = Long.parseLong(lookupValue);
            }else{
                previousDate = 0L;
            }
            for(Object value : list){
                String logValues[] = value.toString().split("\t", 2);
                reducerValue = logValues[1];
                currentDate = Long.parseLong(logValues[0]);
                Long difference = currentDate - previousDate;
                if(difference <= 60 && difference >= 0){
                    reducerValue = reducerValue + "\t" + currentDate + "\t" + difference + "\t" + "SUSPECT";
                }else{
                    reducerValue = reducerValue + "\t" + currentDate + "\t" + difference + "\t" + "VALID";
                }
                context.write(new Text("0"), new Text(reducerValue));
                previousDate = currentDate;
            }
        }else{
            for(Text value : values){
                context.write(new Text("0"), new Text(value.toString() + "\t0\t0\tNA"));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        clickFrequency.clear();
    }
}
