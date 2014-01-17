package at.ac.ait.dme.video_batch.xuggle;

import org.apache.hadoop.conf.Configuration;

import com.xuggle.xuggler.io.IURLProtocolHandler;
import com.xuggle.xuggler.io.IURLProtocolHandlerFactory;

/**
 * Creates HDFSProtocolHandler.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class HDFSProtocolHandlerFactory implements IURLProtocolHandlerFactory {
    
    private Configuration conf;

    public HDFSProtocolHandlerFactory() {
        this.conf = new Configuration();
    }
    /**
     * Constructs Factory and stores given Hadoop Configuration.
     * Maybe be useful.
     */
    public HDFSProtocolHandlerFactory( Configuration conf ) {
        this.conf = conf;
    }

    public IURLProtocolHandler getHandler(String protocol, String url, int flags) {
        if( protocol.equals("hdfs") ) {
            return new HDFSProtocolHandler(conf);
        }
        return null;
    }

}
