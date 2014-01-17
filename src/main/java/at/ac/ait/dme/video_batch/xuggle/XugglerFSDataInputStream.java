/**
 * Class XugglerFSDataInputStream
 * 
 * Wrapper around a Hadoop FSDataInputStream to make it readable to Xuggler's IContainer.
 */
package at.ac.ait.dme.video_batch.xuggle;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

import at.ac.ait.dme.video_batch.VideoBatch;

/**
 * Represents a portion within a video stream which is readable by Xuggler.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class XugglerFSDataInputStream extends InputStream implements Closeable {

    private static final Log LOG = LogFactory
            .getLog(XugglerFSDataInputStream.class);

    /**
     * BUFFER_SIZE needs to be a divisor of HDFS-Blocksize.
     */
    private static int BUFFER_SIZE = 0;// VideoBatch.BUFFER_SIZE;

    /**
     * Stream on HDFS.
     */
    FSDataInputStream fIn;

    /**
     * Starting point to read from FSDataInputStream
     */
    long start;

    /**
     * Ending point to read from FSDataInputStream
     */
    long end;

    /**
     * Caches header bytes of video.
     */
    private byte[] header = null;

    /**
     * Position of this stream (not of the underlying FSDataInputStream!)
     */
    private long pos;

    public XugglerFSDataInputStream(InputStream in, long start, long end)
            throws IOException {
        this.fIn = (FSDataInputStream) in;
        
        this.fIn.seek(start);
        
        this.start = start;
        this.end = end;
        
    }

    public void setHeader(byte[] b) {
        LOG.debug("header set, length: " + b.length);
        this.header = b;
    }

    public boolean isHeaderSet() {
        return this.header != null;
    }

    /**
     * 
     * The first bytes that are read from this stream are the header bytes of
     * the original video stream. After reading them, this stream start to read from the original stream from given start to given end.
     * When reaching the end byte position it behaves like having reached the end of stream (returns -1).
     * 
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read(byte[])
     */
    public int read(byte[] bytes) throws IOException {

        if (!isHeaderSet())
            throw new IOException("Header not set");

        if (isEnd())
            return -1;

        int b = 0;

        LOG.debug("reading " + bytes.length + ", pos = " + pos);

        if (pos < header.length) {
            // copy header
            for (b = (int) pos; b < bytes.length + pos && b < header.length; b++) {
                bytes[b] = (byte) readHeader(b);
            }
            pos = b;
        }

        if (pos >= header.length) {
            int toRead = bytes.length - b;
            long restToEnd = getRestToEnd();
            if (restToEnd < toRead)
                toRead = (int) restToEnd;

            int r = fIn.read(bytes, b, toRead);
            if (r > 0) {
                // if not all requested bytes are read, there maybe a file block border.
                // try again!
                if( r < toRead ) {
                    int r2 = fIn.read(bytes, b + r, toRead - r );
                    if( r2 >= 0 )
                        r += r2;
                }
                pos += r;
                if (b == -1)
                    b = 0;
                b += r;
            } else
                return -1;
        }
        return b;
    }

    /**
     * How many bytes are left to read from this part stream.
     * 
     * @return
     */
    private long getRestToEnd() {
        return end - start - pos + header.length;
    }

    /**
     * 
     * The first bytes that are read from this stream are the header bytes of
     * the original video stream. After reading them, this stream start to read from the original stream from given start to given end.
     * When reaching the end byte position it behaves like having reached the end of stream (returns -1).
     * 
     * (non-Javadoc)
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        // LOG.debug( "reading 1 byte, pos = " + pos );
        if (!isHeaderSet())
            throw new IOException("Header not set");

        if (pos < header.length) {
            return readHeader((int) pos++);
        }
        if (isEnd()) {
            LOG.debug("end reached, pos = " + (pos + start - header.length));
            return -1;
        }

        int b = fIn.read();
        if (b >= 0)
            pos++;
        return b;
    }

    /**
     * Whether end of stream part is reached.
     * 
     * @return
     */
    private boolean isEnd() {
        return pos + start - header.length >= end;
    }

    /**
     * Read a byte from the cache header byte[]. 
     * @param l
     * @return
     */
    private int readHeader(int l) {
        return (int) (header[l] & 0xFF);
    }

    public boolean markSupported() {
        return false;
    }

    public void close() {
        // maybe closing is not a good idea due to side-effects
        
        //try {
            //LOG.debug("closing");
            //fIn.close();
        //} catch (IOException e) {
            //e.printStackTrace();
        //}
    }
    
    public long getPos() {
        return pos;
    }

}
