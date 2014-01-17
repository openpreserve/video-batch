package at.ac.ait.dme.video_batch.xuggle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;

import at.ac.ait.dme.video_batch.AVCompressor;

import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainerFormat;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IRational;
import com.xuggle.xuggler.IStreamCoder;

public class XugglerCompressor extends AVCompressor {

    private IContainerFormat containerFormat;
    private IStreamCoder[] streamCoders;

    public void setInput(byte[] b, int off, int len) {
        // TODO Auto-generated method stub

    }

    public boolean needsInput() {
        // TODO Auto-generated method stub
        return false;
    }

    public void setDictionary(byte[] b, int off, int len) {
        // TODO Auto-generated method stub

    }

    public long getBytesRead() {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getBytesWritten() {
        // TODO Auto-generated method stub
        return 0;
    }

    public void finish() {
        // TODO Auto-generated method stub

    }

    public boolean finished() {
        // TODO Auto-generated method stub
        return false;
    }

    public int compress(byte[] b, int off, int len) throws IOException {
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
     * A quick hack. Encoding paramaters are hard-coded here.
     *
     * @throws IOException
     */
    @Override
    public void setEncodingParameters() throws IOException {
        containerFormat = IContainerFormat.make();
        containerFormat.setOutputFormat("avi", null, null);
        // ObjectOutputStream oos = new ObjectOutputStream();
        // oos.writeObject(containerFormat);

        streamCoders = new IStreamCoder[] { IStreamCoder
                .make(IStreamCoder.Direction.ENCODING) };

        ICodec codec = ICodec.guessEncodingCodec(containerFormat, null, null,
                null, ICodec.Type.CODEC_TYPE_VIDEO);

        streamCoders[0].setNumPicturesInGroupOfPictures(1);
        streamCoders[0].setCodec(codec);

        streamCoders[0].setBitRate(25000);
        streamCoders[0].setBitRateTolerance(9000);
        streamCoders[0].setPixelType(IPixelFormat.Type.YUV420P);
        streamCoders[0].setWidth(352);
        streamCoders[0].setHeight(288);
        streamCoders[0].setFlag(IStreamCoder.Flags.FLAG_QSCALE, true);
        streamCoders[0].setGlobalQuality(0);

        IRational frameRate = IRational.make(25, 1);
        streamCoders[0].setFrameRate(frameRate);
        streamCoders[0].setTimeBase(IRational.make(frameRate.getDenominator(),
                frameRate.getNumerator()));
    }

    /**
     * Not working yet.
     *
     * @param b
     * @param off
     * @param len
     * @throws IOException
     */
    @Override
    public void setContainerFormat(byte[] b, long off, long len)
            throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(b);
        ObjectInputStream ois = null;

        ois = new ObjectInputStream(bis);

        try {
            containerFormat = (IContainerFormat) ois.readObject();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Not working yet.
     *
     * @param b
     * @param off
     * @param len
     * @throws IOException
     */
    @Override
    public void setStreamEncodings(byte[] b, long off, long len)
            throws IOException {

        ByteArrayInputStream bis = new ByteArrayInputStream(b);
        ObjectInputStream ois = null;

        ois = new ObjectInputStream(bis);

        try {
            streamCoders = (IStreamCoder[]) ois.readObject();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public IContainerFormat getContainerFormat() {
        // TODO Auto-generated method stub
        return containerFormat;
    }

    public IStreamCoder[] getStreamCoders() {
        // TODO Auto-generated method stub
        return streamCoders;
    }

    @Override
    public void reinit(Configuration arg0) {
        // TODO Auto-generated method stub

    }

}
