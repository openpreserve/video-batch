package at.ac.ait.dme.video_batch;

import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionOutputStream;

abstract public class AVOutputStream extends CompressionOutputStream {

    protected AVOutputStream(OutputStream out) {
        super(out);
    }
    
    abstract public boolean writePacket( AVPacket packet );


}
