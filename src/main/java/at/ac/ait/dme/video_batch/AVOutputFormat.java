package at.ac.ait.dme.video_batch;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Extension of FileOutputFormat to create AV output files.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class AVOutputFormat extends FileOutputFormat<IntWritable, AVPacket> {
    
    private static final Log LOG = LogFactory.getLog(AVOutputFormat.class);

    CompressionCodec codec;
    
    @Override
    public RecordWriter<IntWritable, AVPacket> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        
        Configuration conf = job.getConfiguration();
        String extension = "";
        
        try {
            codec = (CompressionCodec) Class.forName(conf.get("video_batch.codec")).newInstance();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        // create output destination
        Path file = getDefaultWorkFile(job, extension);
        
        LOG.debug( "output file: " + file );
        
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream out = fs.create( file, null );
        
        // setup encodings from configuration properties
        
        AVCompressor compressor = (AVCompressor)codec.createCompressor();
        
        //byte[] b = conf.get( "video_batch.encoding_parameters" ).getBytes();
        
        // workaround: encoding parameters are hard-coded in XugglerCompressor for now
        compressor.setEncodingParameters();
        
        // first we set container format using compressor's setDictionary method
        //byte[] b = conf.get( "video_batch.container_format" ).getBytes();
        //compressor.setContainerFormat(b, 0, b.length);
        
        // second we set stream encodings using compressor's setDictionary method again
        //b = conf.get( "video_batch.stream_encodings" ).getBytes();
        //compressor.setStreamEncodings(b, 0, b.length);
        
        AVOutputStream fileOut = (AVOutputStream)codec.createOutputStream(out, compressor);
        
        return new AVPacketRecordWriter( fileOut );
    }

}
