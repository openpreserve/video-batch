package at.ac.ait.dme.video_batch.xuggle;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.xuggle.xuggler.io.IURLProtocolHandler;

/**
 * Provides methods for Xuggler to read from HDFS files.
 *
 * @author Matthias Rella, DME-AIT
 */
public class HDFSProtocolHandler implements IURLProtocolHandler {
    
    private static final Log LOG = LogFactory.getLog(HDFSProtocolHandler.class);

    private FSDataInputStream fIn;

    private Path pile;

    private Configuration conf;

    private FileSystem fs;

    public HDFSProtocolHandler() {
        this.conf = new Configuration();
    }

    /**
     * Constructs HDFSProtocolHandler and stores given Hadoop Configuration.
     * Maybe it is needed somewhen, somewhere ...
     * 
     * @param conf
     */
    public HDFSProtocolHandler(Configuration conf) {
        this.conf = conf;
    }

    public HDFSProtocolHandler(String input) {
        // TODO Auto-generated constructor stub
        pile = new Path( input );
        conf = new Configuration();
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#close()
     */
    public int close() {
        // TODO Auto-generated method stub
        try {
            fIn.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#isStreamed(java.lang.String, int)
     */
    public boolean isStreamed(String arg0, int arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    /* 
     * flags not supported. Only Read-only.
     * (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#open(java.lang.String, int)
     */
    public int open(String url, int flags) {
        // if url is not a hdfs-url then return negative value to indicate
        // failure
        
        LOG.debug( "Opening HDFSProtocolHandler with " + url);
        if (url != null && !url.startsWith("hdfs:"))
            return -1;

        if( url != null )
            pile = new Path(url);
        
        if( pile == null ) return -1;
        
        try {
            fs = pile.getFileSystem(conf);
            fIn = fs.open(pile);
        } catch (IOException e) {
            LOG.debug( "HDFSProtocolHandler could not be opened");
            LOG.debug( e.getStackTrace() );
            return -2;
        }
        
        LOG.debug( "HDFSProtocolHandler opened");

        return 0;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#read(byte[], int)
     */
    public int read(byte[] buf, int size) {
        int r = 0;
        try {
            r = fIn.read(buf, 0, size);
            // if could not read all requested bytes, there may be a file block border ?
            // so try again from the current position
            if( r < size )
            {
                int r2 = fIn.read(buf, r, size - r );
                if( r2 >= 0 )
                    r += r2;
            }

            // IUrlProtocolHandler wants return value to be zero if end of file
            // is reached
            if (r == -1)
                r = 0;

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            r = -1;
        }
        return r;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#seek(long, int)
     */
    public long seek(long offset, int whence) {
        LOG.debug( "seeking to " + offset + ", whence = " + whence );
        long pos;
        try {
            FileStatus status = fs.getFileStatus(pile);
            long len = status.getLen();
            
            switch (whence) {
            case SEEK_CUR:
                long old_pos = fIn.getPos();
                fIn.seek(old_pos + offset); 
                pos = old_pos - fIn.getPos();
                break;
            case SEEK_END:
                fIn.seek( len + offset );
                pos = fIn.getPos() - len;
                break;
            case SEEK_SIZE:
                pos = len;
                break;
            case SEEK_SET:
            default:
                fIn.seek( offset ); 
                pos = fIn.getPos();
                break;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error( e.getMessage() );
            //e.printStackTrace();
            return -1;
        } 
        return pos;
    }

    /* (non-Javadoc)
     * @see com.xuggle.xuggler.io.IURLProtocolHandler#write(byte[], int)
     */
    public int write(byte[] buf, int size) {
        // TODO Auto-generated method stub
        return 0;
    }

}
