package at.ac.ait.dme.video_batch;

import java.io.IOException;
import org.apache.hadoop.io.compress.Compressor;

abstract public class AVCompressor implements Compressor {
    abstract public void setContainerFormat( byte[] b, long off, long len ) throws IOException;
    abstract public void setStreamEncodings( byte[] b, long off, long len ) throws IOException;
    abstract public void setEncodingParameters() throws IOException;
}
