package com.ask.nrelate.cassandra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 27/11/13
 * Time: 12:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraInvoker extends Configured implements Tool {
    private static final String JOB_TRACKER = "mapred.job.tracker";
    private static final String JOB_PRIORITY = "mapred.job.priority";
    private static final String JAVA_OPTS = "mapred.child.java.opts";
    private static final String NAME_NODE = "fs.default.name";
    private static final String BASE_PATH = "/usr/";

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new CassandraInvoker(), args);
        System.exit(exitCode);
    }

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
        configuration.set(NAME_NODE, "hdfs://" + nameNode + ":" + args[1]);
        configuration.set(JOB_TRACKER, nameNode + ":" + args[2]);
        configuration.set(JOB_PRIORITY, args[3]);
        configuration.set(JAVA_OPTS, "-Xmx1024m");
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/cassandra-driver-core-2.0.0-beta2.jar"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/lz4-1.2.0.jar"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/guava-15.0.jar"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/netty-3.6.6.Final.jar"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/"), configuration);
        //DistributedCache.addFileToClassPath(new Path("/cache/jars/"), configuration);

        DistributedCache.addFileToClassPath(new Path("/cache/jars/antlr-3.2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/apache-cassandra-2.0.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/apache-cassandra-clientutil-2.0.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/apache-cassandra-thrift-2.0.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/cassandra-driver-core-2.0.0-beta2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/commons-cli-1.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/commons-codec-1.2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/commons-lang3-3.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/compress-lzf-0.8.4.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/concurrentlinkedhashmap-lru-1.3.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/disruptor-3.0.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/guava-15.0.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/high-scale-lib-1.1.2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/jackson-core-asl-1.9.2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/jackson-mapper-asl-1.9.2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/jamm-0.2.5.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/jbcrypt-0.3m.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/jline-1.0.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/json-simple-1.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/libthrift-0.9.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/log4j-1.2.16.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/lz4-1.1.0.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/metrics-core-2.2.0.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/metrics-core-3.0.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/netty-3.6.6.Final.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/servlet-api-2.5-20081211.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/slf4j-api-1.7.2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/slf4j-api-1.7.5.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/slf4j-log4j12-1.7.2.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/snakeyaml-1.11.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/snappy-java-1.0.5.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/snaptree-0.1.jar"), configuration);
        DistributedCache.addFileToClassPath(new Path("/cache/jars/thrift-server-0.3.2.jar"), configuration);





        nameNode = "hdfs://" + nameNode + ":" + args[1];

        Job job = new Job(configuration, "Insert data to Cassandra");
        //DistributedCache.addCacheFile(new URI(nameNode + "/cache/jars/GeoIP.dat"), job.getConfiguration());

        job.setJobName("Insert data to Cassandra");
        job.setWorkingDirectory(new Path("/tmp"));

        job.setJarByClass(CassandraInvoker.class);
        job.setMapperClass(CassandraDataLoadMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(BASE_PATH + timeStamp));
        FileOutputFormat.setOutputPath(job, new Path(BASE_PATH + timeStamp + "/out"));

        return job.waitForCompletion(true) ? 0 : 1;


    }
}
