package at.ac.ait.dme.video_batch.xuggle;

import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import at.ac.ait.dme.video_batch.AVPacket;

import com.xuggle.ferry.IBuffer;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.Utils;

/**
 * A wrapper for Xuggler's IPacket to be usable within Hadoop, eg. provides
 * serialization of the packet for network transmission.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class XugglerPacket extends AVPacket {

    private static final Log LOG = LogFactory.getLog(XugglerPacket.class);

    private StreamType streamType;

    // private IVideoPicture picture;

    private IVideoPicture decoded_object;

    private long frame_no;

    public XugglerPacket() {
    }

    public XugglerPacket(IPacket packet, ICodec.Type type) {
        this.packet = packet;
        setPosition(packet.getPosition());
        setFrameNo(packet.getPts());
        switch (type) {
        case CODEC_TYPE_ATTACHMENT:
            this.streamType = AVPacket.StreamType.UNKNOWN;
            break;
        case CODEC_TYPE_AUDIO:
            this.streamType = AVPacket.StreamType.AUDIO;
            break;
        case CODEC_TYPE_DATA:
            this.streamType = AVPacket.StreamType.UNKNOWN;
            break;
        case CODEC_TYPE_SUBTITLE:
            this.streamType = AVPacket.StreamType.UNKNOWN;
            break;
        case CODEC_TYPE_UNKNOWN:
            this.streamType = AVPacket.StreamType.UNKNOWN;
            break;
        case CODEC_TYPE_VIDEO:
            this.streamType = AVPacket.StreamType.VIDEO;
            break;
        }
    }

    @Override
    public StreamType getStreamType() {
        return streamType;
    }

    public void setDecodedObject(Object obj) {
        if (getStreamType() == StreamType.VIDEO)
            this.decoded_object = (IVideoPicture) obj;
        this.is_decoded = (this.decoded_object != null);
    }

    @Override
    public BufferedImage getBufferedImage() {
        if (isDecoded() && getStreamType() == StreamType.VIDEO)
            return Utils
                    .videoPictureToImage((IVideoPicture) this.decoded_object);
        return null;
    }

    public long getPosition() {
        return position;
    }

    public long getFrameNo() {
        return this.frame_no;
    }

    @Override
    public void setFrameNo(long frame_no) {
        this.frame_no = frame_no;
        // TODO: we should not set Pts and Dts that way. The Coder should calculate the right values.
        if( this.packet != null ) {
	        ((IPacket)this.packet).setPts( frame_no );
	        ((IPacket)this.packet).setDts( frame_no );
        }
        if( this.decoded_object != null )
            ((IVideoPicture)this.decoded_object).setPts( frame_no * 40000 );
    }

    /**
     * Convert a {@link BufferedImage} of any type, to {@link BufferedImage} of
     * a specified type. If the source image is the same type as the target
     * type, then original image is returned, otherwise new image of the correct
     * type is created and the content of the source image is copied into the
     * new image.
     * 
     * @param sourceImage
     *            the image to be converted
     * @param targetType
     *            the desired BufferedImage type
     * 
     * @return a BufferedImage of the specifed target type.
     * 
     * @see BufferedImage
     * @author Robert Harris
     * @source http://wiki.xuggle.com/Encoding_Video_from_a_sequence_of_Images
     */

    public static BufferedImage convertToType(BufferedImage sourceImage,
            int targetType) {
        BufferedImage image;

        // if the source image is already the target type, return the source
        // image

        if (sourceImage.getType() == targetType)
            image = sourceImage;

        // otherwise create a new image of the target type and draw the new
        // image

        else {
            image = new BufferedImage(sourceImage.getWidth(),
                    sourceImage.getHeight(), targetType);
            image.getGraphics().drawImage(sourceImage, 0, 0, null);
        }

        return image;
    }

    private static class MethodComparator implements Comparator<Method> {

        public int compare(Method m1, Method m2) {
            return (m1.getName().compareTo(m2.getName()));
        }

    }

    /**
     * Serializes a Xuggler IPacket.
     * 
     * @param out
     * @throws IOException 
     */
    public void write(DataOutput out) throws IOException {
        LOG.debug("serializing XugglerPacket ...");

        out.writeInt(streamType.getId());

        LOG.debug("serializing int streamType, value = " + streamType.getId());

        out.writeLong(frame_no);

        LOG.debug("serializing long frame_no, value = " + frame_no);

        IPacket ipacket = ((IPacket) packet);

        //IBuffer data = ipacket.getData();
        //byte[] b = data.getByteArray(0, ipacket.getSize());

        //out.writeInt(b.length);
        //out.write(b);

        //LOG.debug("serializing byte[] Data, length = " + b.length);
        
        // serialize all fields which have getters and setters
        Method[] methods = IPacket.class.getDeclaredMethods();
        
        List<Method> methodsList = Arrays.asList( methods );
        
        java.util.Collections.sort( methodsList, new MethodComparator() );
        
        methods = (Method[])methodsList.toArray();

        for (int m = 0; m < methods.length; m++) {
            String prefix = methods[m].getName().substring(0, 3);
            String name;

            if (prefix.startsWith("is")) {
                prefix = prefix.substring(0, 2);
                name = methods[m].getName().substring(2);
            } else
                name = methods[m].getName().substring(3);

            LOG.debug("Method-prefix: " + prefix + ", Method-name: " + name);

            if (prefix.equals("set") || prefix.equals("get")
                    || prefix.startsWith("is")) {

                for (int m2 = m + 1; m2 < methods.length; m2++) {
                    String name2 = methods[m2].getName().substring(3);

                    if (name.equals(name2)) {

                        Method getter = prefix.equals("get")
                                || prefix.startsWith("is") ? methods[m]
                                : methods[m2];
                        String returnType = getter.getReturnType().getName();

                        try {
                            if (returnType.equals("long")) {
                                long value = (Long) getter.invoke(ipacket);
                                LOG.debug("serialize long " + name2 + ", value = " + value);
                                out.writeLong(value);
                            } else if (returnType.equals("int")) {
                                int value = (Integer) getter.invoke(ipacket);
                                LOG.debug("serialize int " + name2 + ", value = " + value);
                                out.writeInt((Integer) getter.invoke(ipacket));
                            } else if (returnType.equals("boolean")) {
                                boolean value = (Boolean) getter
                                        .invoke(ipacket);
                                LOG.debug("serialize boolean " + name2 + ", value = " + value);
                                out.writeBoolean((Boolean) getter
                                        .invoke(ipacket));
                            }

                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } 

                    }
                }
            }
        }
        
        out.writeInt(decoded_object.getPixelType().ordinal() );
        
        out.writeInt( decoded_object.getWidth() );
        out.writeInt( decoded_object.getHeight() );
        
        IBuffer data = decoded_object.getData();
        byte[] b = data.getByteArray( 0, decoded_object.getSize() );
        
        out.writeInt( b.length );
        out.write(b);
        
        LOG.debug( "serialize IVideoPicture" );
        
        // serialize all fields which have getters and setters of IVideoPicture
        methods = IVideoPicture.class.getDeclaredMethods();
        
        methodsList = Arrays.asList( methods );
        
        java.util.Collections.sort( methodsList, new MethodComparator() );
        
        methods = (Method[])methodsList.toArray();

        for (int m = 0; m < methods.length; m++) {
            String prefix = methods[m].getName().substring(0, 3);
            String name;

            if (prefix.startsWith("is")) {
                prefix = prefix.substring(0, 2);
                name = methods[m].getName().substring(2);
            } else
                name = methods[m].getName().substring(3);

            LOG.debug("Method-prefix: " + prefix + ", Method-name: " + name);

            if (prefix.equals("set") || prefix.equals("get")
                    || prefix.startsWith("is")) {

                for (int m2 = m + 1; m2 < methods.length; m2++) {
                    String name2 = methods[m2].getName().substring(3);

                    if (name.equals(name2)) {

                        Method getter = prefix.equals("get")
                                || prefix.startsWith("is") ? methods[m]
                                : methods[m2];
                        String returnType = getter.getReturnType().getName();

                        try {
                            if (returnType.equals("long")) {
                                long value = (Long) getter.invoke(decoded_object);
                                LOG.debug("serialize long " + name2 + ", value = " + value);
                                out.writeLong(value);
                            } else if (returnType.equals("int")) {
                                int value = (Integer) getter.invoke(decoded_object);
                                LOG.debug("serialize int " + name2 + ", value = " + value);
                                out.writeInt((Integer) getter.invoke(decoded_object));
                            } else if (returnType.equals("boolean")) {
                                boolean value = (Boolean) getter
                                        .invoke(decoded_object);
                                LOG.debug("serialize boolean " + name2 + ", value = " + value);
                                out.writeBoolean((Boolean) getter
                                        .invoke(decoded_object));
                            }

                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } 

                    }
                }
            }
        }

    }

    private void writeObjectFields(DataOutput out, Object obj ) {
        
    }

    /**
     * De-serializes a Xuggler IPacket.
     * 
     * @param in
     * @throws IOException 
     */
    public void readFields(DataInput in) throws IOException {
        LOG.debug("de-serializing XugglerPacket ...");

        streamType = StreamType.create(in.readInt());
        LOG.debug("de-serializing int streamType, value = " + streamType);
        frame_no = in.readLong();
        LOG.debug("de-serializing long frame_no, value = " + frame_no);
        

        IPacket ipacket = IPacket.make(  );

        // de-serialize all fields with getters and setters
        Method[] methods = IPacket.class.getDeclaredMethods();
        
        List<Method> methodsList = Arrays.asList( methods );
        
        java.util.Collections.sort( methodsList, new MethodComparator() );
        
        methods = (Method[])methodsList.toArray();


        for (int m = 0; m < methods.length; m++) {
            String prefix = methods[m].getName().substring(0, 3);

            String name;

            if (prefix.startsWith("is")) {
                prefix = prefix.substring(0, 2);
                name = methods[m].getName().substring(2);
            } else
                name = methods[m].getName().substring(3);

            LOG.debug("Method-prefix: " + prefix + ", Method-name: " + name);

            if (prefix.equals("set") || prefix.equals("get")
                    || prefix.startsWith("is")) {

                for (int m2 = m + 1; m2 < methods.length; m2++) {
                    String name2 = methods[m2].getName().substring(3);

                    if (name.equals(name2)) {

                        Method setter = prefix.equals("set") ? methods[m]
                                : methods[m2];

                        String returnType = setter.getParameterTypes()[0]
                                .getName();

                        try {
                            if (returnType.equals("long")) {
                                long value = in.readLong();
                                LOG.debug("de-serialize long " + name2 + ", value = " + value);
                                setter.invoke(ipacket, value);
                            } else if (returnType.equals("int")) {
                                int value = in.readInt();
                                LOG.debug("de-serialize int " + name2 + ", value = " + value);
                                setter.invoke(ipacket, value);
                            } else if (returnType.equals("boolean")) {
                                boolean value = in.readBoolean();
                                LOG.debug("de-serialize boolean " + name2 + ", value = " + value);
                                // setComplete has got more parameters and needs
                                // special care:
                                if (name.equals("Complete"))
                                    setter.invoke(ipacket, value, ipacket.getSize());
                                else
                                    setter.invoke(ipacket, value);
                            }

                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        }
        
        //((IPacket) packet).setData(IBuffer.make(null, b, 0, len));
        setPacket(ipacket);
        
        LOG.debug( "de-serialize IVideoPicture");
        
        int inType = in.readInt();
        IPixelFormat.Type type = null;
        
        for( IPixelFormat.Type t : IPixelFormat.Type.class.getEnumConstants() ) {
            if( t.ordinal() == inType ) {
                type = t;
                break;
            }
        }
        
        LOG.debug( "de-serialized IPixelFormat, value = " + type.toString() );
        
        int width = in.readInt();
        LOG.debug( "de-serialized Width, value = " + width );
        
        int height = in.readInt();
        LOG.debug( "de-serialized Height, value = " + height );
        
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);

        LOG.debug("de-serializing byte[] data, length = " + len);
        
        IVideoPicture picture = IVideoPicture.make( IBuffer.make(null, b, 0, len ), type, width, height );
        
        // de-serialize all fields with getters and setters of IVideoPicture
        methods = IVideoPicture.class.getDeclaredMethods();
        
        methodsList = Arrays.asList( methods );
        
        java.util.Collections.sort( methodsList, new MethodComparator() );
        
        methods = (Method[])methodsList.toArray();


        for (int m = 0; m < methods.length; m++) {
            String prefix = methods[m].getName().substring(0, 3);

            String name;

            if (prefix.startsWith("is")) {
                prefix = prefix.substring(0, 2);
                name = methods[m].getName().substring(2);
            } else
                name = methods[m].getName().substring(3);

            LOG.debug("Method-prefix: " + prefix + ", Method-name: " + name);

            if (prefix.equals("set") || prefix.equals("get")
                    || prefix.startsWith("is")) {

                for (int m2 = m + 1; m2 < methods.length; m2++) {
                    String name2 = methods[m2].getName().substring(3);

                    if (name.equals(name2)) {

                        Method setter = prefix.equals("set") ? methods[m]
                                : methods[m2];

                        String returnType = setter.getParameterTypes()[0]
                                .getName();

                        try {
                            if (returnType.equals("long")) {
                                long value = in.readLong();
                                LOG.debug("de-serialize long " + name2 + ", value = " + value);
                                setter.invoke(picture, value);
                            } else if (returnType.equals("int")) {
                                int value = in.readInt();
                                LOG.debug("de-serialize int " + name2 + ", value = " + value);
                                setter.invoke(picture, value);
                            } else if (returnType.equals("boolean")) {
                                boolean value = in.readBoolean();
                                LOG.debug("de-serialize boolean " + name2 + ", value = " + value);
                                // setComplete has got more parameters and needs
                                // special care:
                                if (name.equals("Complete")) {
                                    //setter.invoke(picture, value, len);
                                }else
                                    setter.invoke(picture, value);
                            }

                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        }
        setDecodedObject(picture);
    }

    static public XugglerPacket read(DataInput in) throws IOException {
        XugglerPacket p = new XugglerPacket();

        p.readFields(in);

        return p;

    }

    public int compareTo(Integer o) {
        return o.compareTo((int) getPosition());
    }
    
    public Object getDecodedObject() {
        return this.decoded_object;
    }

    @Override
    public void setPacket(IPacket ipacket) {
        this.packet = ipacket;
    }
    
}
