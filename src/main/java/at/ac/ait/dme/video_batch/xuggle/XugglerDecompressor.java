package at.ac.ait.dme.video_batch.xuggle;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;

import at.ac.ait.dme.video_batch.AVPacket;

import com.xuggle.xuggler.Global;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.ICodec.Type;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IError;
import com.xuggle.xuggler.IIndexEntry;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.IVideoResampler;

/**
 * This implementation of Decompressor does not decompress data. It just holds index information.
 * 
 * @author Matthias Rella, DME-AIT
 */

public class XugglerDecompressor implements Decompressor {

    private static final Log LOG = LogFactory.getLog(XugglerDecompressor.class);

    /**
     * The index entries: byte positions and associated frame numbers
     */
    private TreeMap<Long, Long> indices;
    
    /** 
     * Not needed in this implementation.
     * (non-Javadoc)
     * @see org.apache.hadoop.io.compress.Decompressor#setInput(byte[], int, int)
     * 
     */
    public void setInput(byte[] b, int off, int len) {
        // TODO Auto-generated method stub

    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.io.compress.Decompressor#needsInput()
     */
    public boolean needsInput() {
        // TODO Auto-generated method stub
        return false;
    }

    /** 
     * Sets IndexEntries of the whole input video.
     * These are needed to split video data at key frame borders.
     * 
     * (non-Javadoc)
     * @see org.apache.hadoop.io.compress.Decompressor#setDictionary(byte[], int, int)
     */
    public void setDictionary(byte[] b, int off, int len) {
        ByteArrayInputStream bis = new ByteArrayInputStream(b, off, len);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(bis);
            indices = (TreeMap<Long, Long>) ois.readObject();
            LOG.debug("De-serialized indices: size = " + indices.size());
            
            
            if( LOG.isDebugEnabled() ) {
                Set<Entry<Long,Long>> set = indices.entrySet();
                Iterator<Entry<Long,Long>> it = set.iterator();
                while(it.hasNext()) 
                    LOG.debug( it.next() );
            }
            
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

    }

    public boolean needsDictionary() {
        return indices == null || indices.isEmpty();
    }

    public boolean finished() {
        // TODO Auto-generated method stub
        return false;
    }

    public int decompress(byte[] b, int off, int len) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    public void reset() {
        // TODO Auto-generated method stub
    }

    public void end() {
        // TODO Auto-generated method stub
    }

    /**
     * Finds position of an IndexEntry which is next to a given raw byte position of the video data.
     * @param pos
     * @return
     */
    public long getNearestIndexBefore(long pos) {
        //long nearest = 0L;
        
        return indices.floorKey(pos);
        /*
        Iterator<Long> it = indices.values().iterator();
        while( it.hasNext() ) {
            long p = it.next();

            if (nearest == 0 && p <= pos) {
                nearest = p;
            }
        }

        return nearest;
        */
    }
    
    /**
     * Gets size of header by the first index entry position.
     * @return 
     */

    public long getHeaderSize() {
        if( needsDictionary() )
            return 0;
        
        return indices.firstKey();
    }
    
    /**
     * Returns frame number at the given byte position of an index entry.
     * @param key_start
     * @return
     */
    public long getFrameAt(long key_start) {
        return indices.get( key_start );
    }

    @Override
    public int getRemaining() {
        // TODO Auto-generated method stub
        return 0;
    }

}
