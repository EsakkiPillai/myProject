package com.ask.nrelate.fraudelent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * User: Kaniyarasu.D
 * Date: 24/7/13
 * Time: 3:16 PM
 */
public class FraudulentIPDetectionInvokerWODomain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.print("Invalid Number of Arguments.");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration configuration = new Configuration();
        
        configuration.set("fs.default.name", "hdfs://" + args[0]  + ":10011");
        configuration.set("mapred.job.tracker", args[0] + ":10012");
        configuration.set("mapred.output.compress" ,"false");
        
        Job job = new Job(configuration, "Test Fraud");

        job.setJarByClass(FraudulentIPDetectionInvokerWODomain.class);
        job.setMapperClass(FraudulentIPDetectionMapperWODomain.class);
        job.setReducerClass(FraudulentIPDetectionReducerWODomain.class);
        //job.setPartitionerClass(FraudulentIPDetectionPartitioner.class);
        
        job.setOutputKeyClass(FraudulentIPDetectionKeyWODomain.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(10);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/ask/home/nrelate/hdfs/pixel/mr/normalizer_fraudulent/" + args[1]));
        FileOutputFormat.setOutputPath(job, new Path("/ask/home/nrelate/hdfs/pixel/mr/normalizer_fraudulent/" + args[1] + "/aggout"));

        MultipleOutputs.addNamedOutput(job, "timeSlotLevelAggregation", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "dayLevelAggregation", TextOutputFormat.class, Text.class, Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new FraudulentIPDetectionInvokerWODomain(), args);
        System.exit(exitCode);
    }
}
