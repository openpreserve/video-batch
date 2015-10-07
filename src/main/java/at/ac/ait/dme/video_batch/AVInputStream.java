package at.ac.ait.dme.video_batch;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;

import at.ac.ait.dme.video_batch.hadoop20workaround.SplitCompressionInputStream;

//import org.apache.hadoop.io.compress.SplitCompressionInputStream;

//rainer: public abstract class AVInputStream extends SplitCompressionInputStream{
public abstract class AVInputStream extends org.apache.hadoop.io.compress.SplitCompressionInputStream {

    public AVInputStream(InputStream in, long start, long end)
            throws IOException {
        super(in, start, end);
        // TODO Auto-generated constructor stub
    }

    public abstract AVPacket readPacket();

    public abstract boolean finished();

    public abstract void decode(AVPacket obj);

    public abstract BufferedImage readVideoFrame();
    
    public abstract long getPos();

}
