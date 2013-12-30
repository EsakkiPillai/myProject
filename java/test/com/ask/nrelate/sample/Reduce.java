package com.ask.nrelate.sample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Reduce extends Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable intWritable : values){
            sum += intWritable.get();
        }
        context.write(key, new IntWritable(sum));
    }

    public void reduce(Text key,Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}