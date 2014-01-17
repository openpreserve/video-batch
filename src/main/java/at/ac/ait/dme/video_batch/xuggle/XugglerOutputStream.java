package at.ac.ait.dme.video_batch.xuggle;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import at.ac.ait.dme.video_batch.AVOutputStream;
import at.ac.ait.dme.video_batch.AVPacket;

import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IError;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;

/**
 * Represents a output stream which is written to using Xuggler.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class XugglerOutputStream extends AVOutputStream {
    
    private static final Log LOG = LogFactory.getLog(XugglerOutputStream.class);

    private IContainer outContainer;
    private XugglerCompressor compressor;
    private IStream[] outStreams;

    protected XugglerOutputStream(OutputStream out) {
        super(out);
    }
    
    public XugglerOutputStream( OutputStream out, XugglerCompressor compressor ) {
        super(out);
        
        outContainer = IContainer.make();
        this.compressor = compressor;

        int retval = outContainer.open(out, compressor.getContainerFormat() );
        if (retval <0)
            throw new RuntimeException("could not open output stream");
        
        IStreamCoder[] streamCoders = compressor.getStreamCoders();
        
        outStreams = new IStream[ streamCoders.length ];
        for( int i = 0; i < streamCoders.length; i++ ) {
            outStreams[i] = outContainer.addNewStream(i);
            outStreams[i].setStreamCoder( streamCoders[i] );
            streamCoders[i].open();
        }
        
        outContainer.writeHeader();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void finish() throws IOException {
        outContainer.writeTrailer();
    }

    @Override
    public void resetState() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void write(int b) throws IOException {
        // TODO Auto-generated method stub

    }
    
    public void encode( AVPacket packet ) {
        IPacket ipacket = (IPacket)packet.getPacket();
        int streamNum = ipacket.getStreamIndex();
        ipacket = IPacket.make();
        printPacketInfo(ipacket);
        IVideoPicture picture = (IVideoPicture)packet.getDecodedObject();
        if( picture == null ) {
            LOG.debug("Picture is null" );
            return;
        } else
        {
            printPictureInfo(picture);
        }
        if( streamNum < outStreams.length ) {
            LOG.debug( "Coder Bitrate: " + outStreams[streamNum].getStreamCoder().getBitRate());
            LOG.debug( "Coder Name: " + outStreams[streamNum].getStreamCoder().getCodec().getName());
            
            int retVal = outStreams[streamNum].getStreamCoder().encodeVideo( ipacket, picture, -1);
            LOG.debug("encoding, retval = " + retVal );
            if( ipacket.isComplete() )
            {
                //packet = new XugglerPacket( ipacket, ICodec.Type.CODEC_TYPE_VIDEO );
                packet.setPacket( ipacket );
            }
        }
    }

    @Override
    public boolean writePacket(AVPacket packet) {
        
        encode( packet );
        
       
        if( packet instanceof XugglerPacket ) {
            IPacket ipacket = (IPacket)packet.getPacket();
            if (ipacket.isComplete()) {
                if( ipacket.getStreamIndex() < outStreams.length )
                {
                    int retVal = outStreams[ ipacket.getStreamIndex() ].stampOutputPacket( ipacket );
                    LOG.debug( "stampOutputPacket = " + retVal );
                }
                int retVal = outContainer.writePacket(ipacket);
                printPacketInfo(ipacket);
                if( retVal < 0 ) {
                    IError error = IError.make(retVal);
                    LOG.debug( "Error: " + error.getDescription() );
                }
                return retVal >= 0;
            }
        }
        return false;
    } 
    
    public boolean writeBufferedImage( BufferedImage image ) {
    BufferedImage worksWithXugglerBufferedImage = XugglerPacket.convertToType(
                image, BufferedImage.TYPE_3BYTE_BGR);

        IPacket packet = IPacket.make();
        //IConverter converter = ConverterFactory.createConverter(
                //worksWithXugglerBufferedImage, compressor.);
        return false;
    }
        
    private void printPacketInfo(IPacket packet) {
        String packet_info = "";
        packet_info += "Packet: ";
        packet_info += String.format("pts = %d;", packet.getPts());
        packet_info += String.format("position = %d;", packet.getPosition());
        packet_info += String.format("isKey = %s;", packet.isKey());
        packet_info += String.format("stream_index = %d;",
                packet.getStreamIndex());
        packet_info += String.format("size = %d;", packet.getSize());
        //System.out.println(packet_info);
        LOG.debug(packet_info);
    }
    
    private void printPictureInfo(IVideoPicture picture) {
        LOG.debug( "Picture: Width = " + picture.getWidth() + ", Height = "+picture.getHeight()+", Pts = " + picture.getPts() );
    }

}
