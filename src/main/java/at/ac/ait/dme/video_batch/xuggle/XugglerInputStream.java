package at.ac.ait.dme.video_batch.xuggle;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

import at.ac.ait.dme.video_batch.AVInputStream;
import at.ac.ait.dme.video_batch.AVPacket;

import com.xuggle.xuggler.Global;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.ICodec.Type;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IError;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.IVideoResampler;

/**
 * Represents a input stream where AV data is read from using Xuggler.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class XugglerInputStream extends AVInputStream {

    private static final Log LOG = LogFactory.getLog(XugglerInputStream.class);

    private XugglerDecompressor decompressor;

    private long frame_offset;

    private IContainer container;

    private IStream[] streams;

    private int videoStreamId;

    private IStreamCoder coder[];

    private IVideoResampler resampler;

    private XugglerFSDataInputStream xfsdis;

    private Type[] streamTypes;

    private boolean finished;
    
    public static int PACKET_READS_BEFORE_EOF_ASSUMED = 20;
    
    private int packetReads = 0;

    public XugglerInputStream(InputStream in, long start, long end, long frame_offset, XugglerDecompressor dec)
            throws IOException {
        super(in, start, end);
        
        this.frame_offset = frame_offset;

        decompressor = dec;
        
        FSDataInputStream fIn = (FSDataInputStream) in;
        
        int header_size = (int) dec.getHeaderSize();
        byte[] header = new byte[header_size];
        
        // read header 
        fIn.seek(0);
        int b = fIn.read(header, 0, header_size);
        LOG.info( "header read, count bytes: " + b );
        LOG.info( "after header reading, fIn.pos = " + fIn.getPos() );
        LOG.info("creating xfsdis start: "+start+" end "+end);
        
        xfsdis = new XugglerFSDataInputStream(in, start, end);
        
        LOG.info("setting header");
        xfsdis.setHeader( header );
        
        LOG.info("making container");
        this.container = IContainer.make();

        // IMetaData metadata = container.getMetaData();
        // format.setInputFlag(IContainerFormat.Flags.FLAG_GLOBALHEADER, true);
        // this.container.setParameters(parameters);
        
        LOG.info("opening container");
        int r = this.container.open( xfsdis, null);
        
        LOG.info("container.open = " + r);

        if (r < 0) {
            IError error = IError.make(r);
            LOG.info("error: " + error.getDescription());
            throw new IOException( "Could not create Container from given InputStream");
        }
        
        // store container info: streams, coders ... whatever is needed later for decoding
        int numStreams = this.container.getNumStreams();
        this.streams = new IStream[numStreams];
        this.coder = new IStreamCoder[numStreams];
        this.streamTypes = new Type[numStreams];
        
        for (int s = 0; s < this.streams.length; s++) {
            this.streams[s] = this.container.getStream(s);
            printStreamInfo(streams[s]);
            this.coder[s] = this.streams[s] != null ? this.streams[s].getStreamCoder() : null;
            this.streamTypes[s] = this.coder[s] != null ? this.coder[s].getCodecType() : null;
        }
        //testDecoding();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void resetState() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    public AVPacket readPacket() {
        IPacket packet = IPacket.make();
        XugglerPacket xpacket = null;

        if (this.container.readNextPacket(packet) >= 0) {
            printPacketInfo(packet);

            xpacket = new XugglerPacket(packet,
                    streamTypes[packet.getStreamIndex()]);
        } else {
        		LOG.info("read empty package - tries: "+packetReads++);
      			LOG.info("packet is: "+packet);
        		if(packetReads >= PACKET_READS_BEFORE_EOF_ASSUMED) {
        			LOG.info("assuming no more packets to read - setting finished flag");
        			finished = true;
        		}
        }

        if (xpacket != null) {
            // calc position of the packet relative to the whole video
            long pos = xpacket.getPosition() - decompressor.getHeaderSize()
                    + getAdjustedStart();
            xpacket.setPosition(pos);
            LOG.info("adding frame offest: " + getFrameOffset());            
            xpacket.setFrameNo(xpacket.getFrameNo() + getFrameOffset());
            LOG.info("readPacket, frame no " + xpacket.getFrameNo());
        }
        return xpacket;
    }
    
    /**
     * Convenient method to read a BufferedImage Video Frame directly.
     */
    public BufferedImage readVideoFrame() {
        XugglerPacket xpacket = (XugglerPacket) readPacket();
        
        // search for video packet
        while( xpacket != null && xpacket.getStreamType() != AVPacket.StreamType.VIDEO )
            xpacket = (XugglerPacket)readPacket();
        
        // reached end of stream
        if( xpacket == null && finished() ) return null;
        
        // if obj is still no video packet, there is no video packet
        if( xpacket.getStreamType() != AVPacket.StreamType.VIDEO ) return null;
        
        if( !xpacket.isDecoded())
        	//There is one video packet per Image
            decode( xpacket );
        
        return xpacket.getBufferedImage();
    }

    public long getFrameOffset() {
        return frame_offset;
    }

    public void decode(AVPacket packet) {
        IPacket p = (IPacket) packet.getPacket();
        packet.setDecodedObject(decodePacket(p));
    }

    public Object decodePacket(IPacket packet) {

        LOG.debug("decodePacket");

        if (streamTypes[packet.getStreamIndex()] == ICodec.Type.CODEC_TYPE_VIDEO)
            return decodeVideoPacket(packet);
        if (streamTypes[packet.getStreamIndex()] == ICodec.Type.CODEC_TYPE_AUDIO)
            return decodeAudioPacket(packet);

        return null;

    }

    private Object decodeAudioPacket(IPacket packet) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Decode a Video Packet. Taken from Xuggler Demo file for Video decoding.
     * @param packet
     * @return
     */
    private IVideoPicture decodeVideoPacket(IPacket packet) {

        int c = 0;
        for (c = 0; c < coder.length; c++)
            if (coder[c] != null
                    && coder[c].getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO)
                break;

        if (c >= coder.length)
            return null;

        IStreamCoder videoCoder = coder[c];

        /*
         * We allocate a new picture to get the data out of Xuggler
         */
        IVideoPicture picture = IVideoPicture.make(videoCoder.getPixelType(),
                videoCoder.getWidth(), videoCoder.getHeight());

        if (!videoCoder.isOpen()) {
            int o = videoCoder.open();
            if (o < 0)
                LOG.debug("error on open VideoCoder = " + o);
            //setResampler(videoCoder);
        }

        int offset = 0;
        while (offset < packet.getSize()) {
            /*
             * Now, we decode the video, checking for any errors.
             */
            int bytesDecoded = videoCoder.decodeVideo(picture, packet, offset);
            if (bytesDecoded < 0)
                throw new RuntimeException("got error decoding video in: "
                        + packet.getPosition());
            offset += bytesDecoded;

            /*
             * Some decoders will consume data in a packet, but will not be able
             * to construct a full video picture yet. Therefore you should
             * always check if you got a complete picture from the decoder
             */
            if (picture.isComplete()) {
                IVideoPicture newPic = picture;
                /*
                 * If the resampler is not null, that means we didn't get the
                 * video in BGR24 format and need to convert it into BGR24
                 * format.
                 */
                if (resampler != null) {
                    // we must resample
                    newPic = IVideoPicture.make(
                            resampler.getOutputPixelFormat(),
                            picture.getWidth(), picture.getHeight());
                    if (resampler.resample(newPic, picture) < 0)
                        throw new RuntimeException(
                                "could not resample video from: "
                                        + packet.getPosition());
                }

                return newPic;
            }
        }
        LOG.warn("Failed to decode Video Packet; Returning null!"); 
        return null;
    }

    private void setResampler(IStreamCoder videoCoder) {
        if (videoCoder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
            return;

        resampler = null;
        if (videoCoder.getPixelType() != IPixelFormat.Type.BGR24) {
            // if this stream is not in BGR24, we're going to need to
            // convert it. The VideoResampler does that for us.
            resampler = IVideoResampler.make(videoCoder.getWidth(),
                    videoCoder.getHeight(), IPixelFormat.Type.BGR24,
                    videoCoder.getWidth(), videoCoder.getHeight(),
                    videoCoder.getPixelType());
            if (resampler == null)
                throw new RuntimeException("could not create color space "
                        + "resampler for: " + videoCoder);
        }
    }

    public boolean finished() {
        return finished;
    }

    private void printStreamInfo(IStream stream) {
        IStreamCoder coder = stream.getStreamCoder();
        IContainer container = stream.getContainer();
        String info = "";

        info += (String.format("type: %s; ", coder.getCodecType()));
        info += (String.format("codec: %s; ", coder.getCodecID()));
        info += String.format(
                "duration: %s; ",
                stream.getDuration() == Global.NO_PTS ? "unknown" : ""
                        + stream.getDuration());
        info += String.format("start time: %s; ",
                container.getStartTime() == Global.NO_PTS ? "unknown" : ""
                        + stream.getStartTime());
        info += String
                .format("language: %s; ",
                        stream.getLanguage() == null ? "unknown" : stream
                                .getLanguage());
        info += String.format("timebase: %d/%d; ", stream.getTimeBase()
                .getNumerator(), stream.getTimeBase().getDenominator());
        info += String.format("coder tb: %d/%d; ", coder.getTimeBase()
                .getNumerator(), coder.getTimeBase().getDenominator());

        if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_AUDIO) {
            info += String.format("sample rate: %d; ", coder.getSampleRate());
            info += String.format("channels: %d; ", coder.getChannels());
            info += String.format("format: %s", coder.getSampleFormat());
        } else if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
            info += String.format("width: %d; ", coder.getWidth());
            info += String.format("height: %d; ", coder.getHeight());
            info += String.format("format: %s; ", coder.getPixelType());
            info += String.format("frame-rate: %5.2f; ", coder.getFrameRate()
                    .getDouble());
        }
        LOG.info(info);
    }

    private void printPacketInfo(IPacket packet) {
        String packet_info = "";
        packet_info += "Packet: ";
        packet_info += String.format("position = %d;", packet.getPosition());
        packet_info += String.format("presentation time stamp (pts) = %d;", packet.getPts());
        packet_info += String.format("decompression time stamp (pts) = %d;", packet.getDts());
        packet_info += String.format("isKey = %s;", packet.isKey());
        packet_info += String.format("stream_index = %d;",
                packet.getStreamIndex());
        packet_info += String.format("size = %d;", packet.getSize());
        LOG.info(packet_info);
    }

    @Override
    public long getPos() {
        return xfsdis.getPos() - decompressor.getHeaderSize() + getAdjustedStart();
    }

}
