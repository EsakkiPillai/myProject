package com.ask.nrelate.sample;

//import com.ask.nrelate.ron.mr.LogNormalizerMapper;
//import com.ask.nrelate.ron.mr.LogNormalizerReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map;


public class WordCount{


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:10011");
        configuration.set("mapred.job.tracker","localhost:10012");

        Job job = new Job(configuration, "Word Count");

        job.setJarByClass(WordCount.class);
        //job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Submit the job to the cluster and wait for it to finish.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

