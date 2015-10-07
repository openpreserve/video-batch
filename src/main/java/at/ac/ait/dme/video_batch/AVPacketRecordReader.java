package at.ac.ait.dme.video_batch;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//rainer: import at.ac.ait.dme.video_batch.hadoop20workaround.SplitCompressionInputStream;
//rainer: import at.ac.ait.dme.video_batch.hadoop20workaround.SplittableCompressionCodec;

/**
 * Reads frame numbers and AVPackets from an underlying compressed audio-video stream.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class AVPacketRecordReader extends RecordReader<IntWritable, AVPacket> {
    
    private static final Log LOG = LogFactory.getLog(AVPacketRecordReader.class);

    private IntWritable key = null;

    //private BufferedImage value = null;
    private AVPacket value = null;

    private CompressionCodec codec;

    private long start;

    private long end;
    
    private long pos;

    private Decompressor decompressor;

    private AVInputStream in = null;


    /**
     *  
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration conf = context.getConfiguration();
        Job job = new Job(conf);

        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        try {
            codec = (CompressionCodec) Class.forName(conf.get("video_batch.codec")).newInstance();
        } catch (Exception e) {
            // should not happen! Codec availability must be checked in advance
            e.printStackTrace();
            throw new InterruptedException();
        }
        
        LOG.info( "Input File: " + file );
        LOG.info( "Using codec: " + codec );
        
        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file);
        
        decompressor = codec.createDecompressor();
        
        String index_file_path =  conf.get( "video_batch.index_file_path");
        String index_file_prefix = conf.get( "video_batch.index_file_prefix" );
        
        // de-serialize indices
        byte[] b = null;
        //URI[] files = job.getCacheFiles();
        // hadoop 0.20 way:
        // read index file from HDFS directly. No DistributedCache used.
        
        // does not work:
        //URI[] files = DistributedCache.getCacheFiles(conf);
        
        String index_file = index_file_path + Path.SEPARATOR + index_file_prefix + file.getName();
        LOG.debug( "index_file = " + index_file );
        
        Path pile = new Path( index_file );
        FSDataInputStream fdis = fs.open( pile );
        
        FileStatus status = fs.getFileStatus( pile );
        
        long index_file_length = status.getLen();
        
        b = new byte[(int)index_file_length];
        fdis.read(b);
        
        
        /*
        for( URI f : files ) {
            LOG.debug( "Cached URI: " + f.toString() );
            if( conf.get( "video_batch.index_file_name" ).equals( f.toString() ))
            {
                File index_file = new File( f );
                if( !index_file.exists() || index_file.length() == 0 ) break;
                b = new byte[(int)index_file.length()];
                FileInputStream fis = new FileInputStream(index_file);
                //ByteArrayInputStream bis = new ByteArrayInputStream( fis );
                fis.read(b);
                break;
            }
        }
        */
                
        // pass index entries data to decompressor
        if( b != null && decompressor.needsDictionary() )
            decompressor.setDictionary(b, 0, b.length);
        
        if (codec instanceof SplittableCompressionCodec) {
            
            LOG.debug( "start = " + start + ", end = " + end);
            
            final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec)
                    .createInputStream(fileIn, decompressor, start, end,
                            SplittableCompressionCodec.READ_MODE.BYBLOCK);
            if( cIn != null ) {
	            start = cIn.getAdjustedStart();
	            end = cIn.getAdjustedEnd();
	            LOG.debug( "Adjusted start = " + start + ", end = " + end);
	            in = (AVInputStream)cIn;
            }
        }
        this.pos = start;
        
    }

    /**
     * Reads next frame number and AVPacket from input split stream.
     * 
     * @return
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        LOG.debug( "nextKeyValue" );
        
        key = null;
        value = null;
        
        if( in == null ) return false;
        
        AVPacket obj = null;
        
        obj = in.readPacket();
        
        
        
        // search for video packet
        while( obj != null && obj.getStreamType() != AVPacket.StreamType.VIDEO )
            obj = in.readPacket();
        
        // reached end of stream
        if( obj == null && in.finished() ) return false;
        
        // if obj is still no video packet, there is no video packet
        //if( obj.getStreamType() != AVPacket.StreamType.VIDEO ) return false;
        
        try {
	        if( !obj.isDecoded())
	            in.decode( obj );
        } catch (Exception e) {
        	LOG.warn("Error decoding AVPacket: "+e);
        }
        /*
        value = obj.getBufferedImage();
        
        // video could not be decoded. ignore it and move on
        if( value == null ) 
        {
            LOG.debug( "value is NULL!" );
            return true;
        }
        */
        
        value = obj;
        
        
        //key = new Text( obj.getFrameNo() + "" );
        key = new IntWritable( (int) obj.getFrameNo() );
        
        return true;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public AVPacket getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    /**
     * Progress of reading the split stream.
     * 
     * @return
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        float ret = 0f;
        if( in == null || in.finished() )
            ret = 1;
        else
            ret = (float)( in.getPos() - start ) / (end - start);
        LOG.debug( "getProgress: " + ret );
        return ret;
    }

    @Override
    public void close() throws IOException {
        // maybe closing is not a good idea due to side effects
        //if( in != null ) in.close();
    }

}
