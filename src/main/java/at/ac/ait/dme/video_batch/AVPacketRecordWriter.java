package at.ac.ait.dme.video_batch;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes AVPackets to an underlying AVOutputStream.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class AVPacketRecordWriter extends RecordWriter<IntWritable, AVPacket> {
    
    private static final Log LOG = LogFactory.getLog(AVPacketRecordWriter.class);

    CompressionCodec codec;

    AVOutputStream out;

    public AVPacketRecordWriter(AVOutputStream fileOut) {
        out = fileOut;
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
            InterruptedException {
        out.close();

    }

    @Override
    public void write(IntWritable id, AVPacket packet ) throws IOException,
            InterruptedException {
        LOG.debug( "writing Packet" );
        boolean retVal = out.writePacket(packet);
        LOG.debug( "was " + ( retVal ? "" : "not" ) + " successful");

    }

}
