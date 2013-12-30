package com.ask.nrelate.fraudelent;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: Kaniyarasu.D
 * Date: 25/7/13
 * Time: 1:20 PM
 */
public class FraudulentIPDetectionPartitioner extends Partitioner<FraudulentIPDetectionKey, Text>{

    @Override
    public int getPartition(FraudulentIPDetectionKey fraudulentIPDetectionKey, Text text, int numPartitions) {
        int hash = fraudulentIPDetectionKey.hashCode();
        int partition = Math.abs(hash) % numPartitions;
        return partition;
    }
}
