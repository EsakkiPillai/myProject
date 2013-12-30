package com.ask.nrelate.fraudelent;

import com.ask.nrelate.utils.StringConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

/**
 * User: Kaniyarasu.D
 * Date: 24/7/13
 * Time: 12:02 PM
 */
public class FraudulentIPDetectionReducerWODomain extends Reducer<FraudulentIPDetectionKeyWODomain, Text, Text, Text> {
    private MultipleOutputs<Text, Text> reducerOutputService;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        reducerOutputService = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void reduce(FraudulentIPDetectionKeyWODomain key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, ReducerOutput> reducerOutputs =  new HashMap<String, ReducerOutput>(48);
        int fullEventCount = 0, eventCount = 1;
        double suspectScore = 0;
        List<String> listValues = new ArrayList<String>();
        long meanFrequency = 0L, previousOccurrence = 0L;
        String firstPlugin = "";

        for(Text value: values){
            listValues.add(value.toString());
        }

        Collections.sort(listValues, new Comparator<String>() {
            @Override
            public int compare(String s, String s1) {
                long timeInMillis = Long.parseLong(s.split(StringConstants.tab)[1]);
                long timeInMillis1 = Long.parseLong(s1.split(StringConstants.tab)[1]);
                if(timeInMillis > timeInMillis1)
                    return 1;
                if(timeInMillis < timeInMillis1)
                    return -1;
                return 0;
            }
        });

        if(listValues.size() > 0){
            for(String currentValue : listValues){
                String[] currentValues = currentValue.split(StringConstants.tab);
                String currentTimeSlot = currentValues[0];
                long currentTimeInMillis = Long.parseLong(currentValues[1]);
                if(reducerOutputs.containsKey(key.toString() + currentTimeSlot)){
                    if(firstPlugin.equals(currentValues[2])) {
                        meanFrequency += currentTimeInMillis - previousOccurrence;
                        eventCount++;
                        reducerOutputs.get(key.toString() + currentTimeSlot).setMeanFreq(meanFrequency);
                        reducerOutputs.get(key.toString() + currentTimeSlot).setEventCount(eventCount);
                    } else{
                        continue;
                    }
                }else{
                    meanFrequency = 0L;
                    firstPlugin = currentValues[2];
                    eventCount = 1;
                    reducerOutputs.put(key.toString() + currentTimeSlot, new ReducerOutput(key.toString(), eventCount, meanFrequency,
                            Integer.parseInt(currentTimeSlot)));
                }
                previousOccurrence = currentTimeInMillis;
                fullEventCount ++;
            }
            for(String reducerOutput : reducerOutputs.keySet()){
                int tmpEventCount = reducerOutputs.get(reducerOutput).getEventCount();
                reducerOutputs.get(reducerOutput).setMeanFreq(reducerOutputs.get(reducerOutput).getMeanFreq()/(tmpEventCount < 3 ? 1 : tmpEventCount-1));
                double contribution = (double) reducerOutputs.get(reducerOutput).getEventCount()/fullEventCount;
                reducerOutputs.get(reducerOutput).setContribution(contribution);
                if(reducerOutputs.get(reducerOutput).getMeanFreq() != 0){
                    suspectScore += reducerOutputs.get(reducerOutput).getEventCount() / reducerOutputs.get(reducerOutput).getMeanFreq();
                }else{
                    suspectScore += reducerOutputs.get(reducerOutput).getEventCount();
                }

                reducerOutputService.write("timeSlotLevelAggregation",
                        new Text(reducerOutputs.get(reducerOutput).getTimeSlot() + StringConstants.tab
                        + key.toString()), new Text(reducerOutputs.get(reducerOutput).constructTimeSlotOutput()));
            }

            reducerOutputService.write("dayLevelAggregation", new Text(key.toString()), new Text(suspectScore + StringConstants.tab + fullEventCount));
        }


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        reducerOutputService.close();
    }

    public FraudulentIPDetectionReducerWODomain() {
        super();
    }
}

class ReducerOutputWODomain {
    private String key;
    private int eventCount, timeSlot;
    private double meanFreq, contribution, suspectScore;

    ReducerOutputWODomain() {
        eventCount = 1;
        meanFreq = contribution = suspectScore = 0.0;
    }

    ReducerOutputWODomain(String key, int eventCount, double meanFreq, int timeSlot) {
        this.key = key;
        this.eventCount = eventCount;
        this.meanFreq = meanFreq;
        this.timeSlot = timeSlot;
    }

    public String constructTimeSlotOutput(){
        return eventCount
                + StringConstants.tab + meanFreq
                + StringConstants.tab + contribution;
    }

    public String constructDayAggregationOutput(){
        return "";
    }

    public int getTimeSlot() {
        return timeSlot;
    }

    public void setTimeSlot(int timeSlot) {
        this.timeSlot = timeSlot;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getMeanFreq() {
        return meanFreq;
    }

    public void setMeanFreq(double meanFreq) {
        this.meanFreq = meanFreq;
    }

    public int getEventCount() {
        return eventCount;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    public double getContribution() {
        return contribution;
    }

    public void setContribution(double contribution) {
        this.contribution = contribution;
    }

    public double getSuspectScore() {
        return suspectScore;
    }

    public void setSuspectScore(double suspectScore) {
        this.suspectScore = suspectScore;
    }
}


