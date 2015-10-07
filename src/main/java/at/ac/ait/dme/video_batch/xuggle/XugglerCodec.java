package at.ac.ait.dme.video_batch.xuggle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.omg.CORBA.portable.IndirectionException;

//import org.apache.hadoop.io.compress.SplitCompressionInputStream;
//import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

/**
 * Implements a codec for splitted input streams to be used by Xuggler.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class XugglerCodec implements SplittableCompressionCodec {
    private static final Log LOG = LogFactory.getLog(XugglerCodec.class);

    private String default_extension = "avi";

    public CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    public CompressionOutputStream createOutputStream(OutputStream out,
            Compressor compressor) throws IOException {
        // TODO Auto-generated method stub
        if( compressor instanceof XugglerCompressor )
            return new XugglerOutputStream( out, (XugglerCompressor)compressor );
        else
            return null;
    }

    public Class<? extends Compressor> getCompressorType() {
        // TODO Auto-generated method stub
        return XugglerCompressor.class;
    }

    public Compressor createCompressor() {
        // TODO Auto-generated method stub
        return new XugglerCompressor();
    }

    public CompressionInputStream createInputStream(InputStream in)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    public CompressionInputStream createInputStream(InputStream in,
            Decompressor decompressor) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    public Class<? extends Decompressor> getDecompressorType() {
        // TODO Auto-generated method stub
        return XugglerDecompressor.class;
    }

    public Decompressor createDecompressor() {
        // TODO Auto-generated method stub
        return new XugglerDecompressor();
    }

    public String getDefaultExtension() {
        return default_extension;
    }

    /**
     * Creates a input stream from a given split and makes it readable
     * for Xuggler. It appends the header from the input file to it and
     * moves the stream to the next complete frame.
     * 
     * @param seekableIn
     * @param decompressor
     * @param start
     * @param end
     * @param readMode
     * @return
     * @throws IOException 
     */
    public SplitCompressionInputStream createInputStream(
            InputStream seekableIn, Decompressor decompressor, long start,
            long end, READ_MODE readMode) throws IOException {

        XugglerInputStream xis = null;
        try {
            if (!(decompressor instanceof XugglerDecompressor))
                throw new IOException(
                        "Decompressor must be of type XugglerDecompressor." );
            
            XugglerDecompressor dec = ((XugglerDecompressor)decompressor);
	        //FSDataInputStream fIn = (FSDataInputStream) seekableIn;
            
            long header = dec.getHeaderSize();
            long key_start;
            long key_end;
            
            if (start > 0)
                key_start = dec.getNearestIndexBefore( start );
            else
                key_start = start = header;
            
            
            key_end = dec.getNearestIndexBefore(end );

            if (key_start >= key_end)
                throw new IOException(
                        "Keyframe for start equal or bigger than keyframe for end.");
            
	
	        /*
	        int len = (int) (key_end - key_start + header);
	        LOG.debug("len : " + len);
	        byte[] b = new byte[len];
	
	        // write header
	        fIn.read(b, 0, (int) header);
	
	        // move stream to keyframe
	        fIn.seek(key_start);
	
	        // read part before split border
	        int part_before_split_border = (int) (start - key_start);
	        LOG.debug("part_before_split_border: " + part_before_split_border);
	
	        int r = fIn.read(b, (int) header, part_before_split_border);
	        LOG.debug("read from inputstream: " + r);
	
	        // read part after split border
	        int part_after_split_border = (int) (key_end - start);
	        LOG.debug("part_after_split_border: " + part_after_split_border);
	
	        r = fIn.read(b, (int) (header + part_before_split_border),
	                part_after_split_border);
	        LOG.debug("read from inputstream: " + r);
	        
	        // decompressor gets parsable part of video 
            dec.setInput(b, 0, len);
            
            */
            long frame_offset = dec.getFrameAt(key_start);
            
            xis = new XugglerInputStream(seekableIn, key_start, key_end, frame_offset, dec );

        } catch (IOException e) {
            e.printStackTrace();
        }
        return xis;
    }

}
