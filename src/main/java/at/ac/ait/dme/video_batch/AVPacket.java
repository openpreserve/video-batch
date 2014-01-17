package at.ac.ait.dme.video_batch;

import java.awt.image.BufferedImage;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.xuggle.xuggler.IPacket;

/**
 * Abstract serializable and comparable representation of an AV Packet.
 * 
 * @author Matthias Rella, DME-AIT
 */
public abstract class AVPacket implements Comparable<Integer>, Writable, WritableComparable<Integer> {
    
    public enum StreamType { 
        UNKNOWN (0), 
        VIDEO (1), 
        AUDIO (2);
        
        private int id;
        
        StreamType( int id ) {
            this.id = id;
        }

        public int getId() {
            return this.id;
        }

        public static StreamType create(int id) {
            for( StreamType s : StreamType.values() )
                if( s.id == id ) return s;
            return UNKNOWN;
        }
    }

    protected boolean is_decoded = false;

    public abstract AVPacket.StreamType getStreamType();
    
    protected Object packet;
    protected long position;

    public boolean isDecoded() {
        return is_decoded;
    }
    
    public abstract void setDecodedObject( Object obj );
    
    public abstract Object getDecodedObject( );

    public abstract BufferedImage getBufferedImage();

    public Object getPacket() {
        return packet;
    }

    public abstract long getPosition();

    public void setPosition(long l) {
        position = l;
    }
    
    public abstract long getFrameNo();

    public abstract void setFrameNo(long l);

    public int getLength() {
        // TODO Auto-generated method stub
        return 0;
    }

    public byte[] getBytes() {
        // TODO Auto-generated method stub
        return null;
    }

    public abstract void setPacket(IPacket ipacket);

}
