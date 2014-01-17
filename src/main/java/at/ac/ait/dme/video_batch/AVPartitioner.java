package at.ac.ait.dme.video_batch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A dummy partitioner to decide upon partitioning of the key space before
 * sending to a reducer. For now just for testing purposes.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class AVPartitioner extends Partitioner<IntWritable, AVPacket> {
    
    private static final Log LOG = LogFactory.getLog(AVPartitioner.class);

    /**
     * Returns the partition number (ie. the reducer number the given key and value 
     * go to) by just deviding the key space every 500 frames.
     * @param key
     * @param value
     * @param numPartitions
     * @return 
     */
    @Override
    public int getPartition(IntWritable key, AVPacket value, int numPartitions) {
        // TODO: workaround to test partitioning 
        // we want all frames in-between certain values to be assigned to a partition
        return ( key.get() / 500 ) % numPartitions;
    }

}
