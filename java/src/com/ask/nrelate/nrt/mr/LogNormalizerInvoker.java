package com.ask.nrelate.nrt.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.net.URI;

public class LogNormalizerInvoker extends Configured implements Tool {
    private static final String JOB_TRACKER = "mapred.job.tracker";
    private static final String JOB_PRIORITY = "mapred.job.priority";
    private static final String JAVA_OPTS = "mapred.child.java.opts";
    private static final String NAME_NODE = "fs.default.name";
    private static final String BASE_PATH = "/ask/home/nrelate/hdfs/pixel/mr/normalizer_nrt/";

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 5){
            System.err.print("Invalid Number of Arguments.");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration configuration = new Configuration();
        String nameNode = args[0];
        String timeStamp = args[4];
        String cachePath = "/cache/nrt/" + timeStamp + "/";

        configuration.set(NAME_NODE, "hdfs://" + nameNode + ":" + args[1]);
        configuration.set(JOB_TRACKER, nameNode + ":" + args[2]);
        configuration.set(JOB_PRIORITY, args[3]);
        configuration.set(JAVA_OPTS, "-Xmx1024m");
        //DistributedCache.addArchiveToClassPath(new Path("/cache/jars/"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/geoip-api-1.2.10.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/opencsv-2.3.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/commons-beanutils-1.8.3.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/commons-validator-1.4.0.jar"), configuration);

        nameNode = "hdfs://" + nameNode + ":" + args[1];

        Job job = new Job(configuration, "Log Normalizer NRT");
        DistributedCache.addCacheFile(new URI(nameNode + "/cache/jars/GeoIP.dat"), job.getConfiguration());
        //DistributedCache.addCacheFile(new URI(nameNode + cachePath + "suspectFrequency.dat"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(nameNode + cachePath + "pagetype"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(nameNode + cachePath + "campaignRun"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(nameNode + cachePath + "alldomains_history"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(nameNode + cachePath + "nr_alldomains_nrt"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(nameNode + cachePath + "NRTNonBillableURLPatterns"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(nameNode + cachePath + "NRTContractStatusMapping"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI(nameNode + cachePath + "fraudulentIPFile"), job.getConfiguration());

        job.setJobName("Log Normalizer NRT");
        job.setWorkingDirectory(new Path("/tmp"));

        job.setJarByClass(LogNormalizerInvoker.class);
        job.setMapperClass(LogNormalizerMapper.class);
       // job.setReducerClass(LogNormalizerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(BASE_PATH + timeStamp));
        FileOutputFormat.setOutputPath(job, new Path(BASE_PATH + timeStamp + "/out"));

        MultipleOutputs.addNamedOutput(job, "invalidSourceURL", TextOutputFormat.class, Text.class, Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new LogNormalizerInvoker(), args);
        System.exit(exitCode);
    }


}
