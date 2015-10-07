package at.ac.ait.dme.video_batch;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import at.ac.ait.dme.video_batch.hbase.RGBTable;

/**
 * DummyVideoMapper contains Mapper functionality to be applied for testing purposes.
 * For example, it can be used to generate image files from input video frames or to simulate processing time by producing an artificial delay.
 * 
 * @author Matthias Rella, DME-AIT
 */

public class DummyVideoMapper extends Mapper<IntWritable, AVPacket, Text, IntWritable> {

    private static final Log LOG = LogFactory.getLog(DummyVideoMapper.class);
    
    /**
     * ID of the current Map task. 
     * Can be used to distinguish generated output by this ID.
     */
    private TaskAttemptID attemptID;

    /**
     * Produces an articifial delay.
     * Can be used to simulate a reasonable processing time frame for testing purposes.
     * 
     * @param delay     the delay in milliseconds
     */
    public static void dummyDoSomethingForXMilliSeconds(int delay) {
        long stop_at = System.currentTimeMillis() + delay;
        while (true) {
            long now = System.currentTimeMillis();
            if (stop_at < now)
                break;
        }

    }

    /**
     * Generates a String of zeros to be prepended to a given number in order to create digit strings of unified length.
     * This is used for generation of sortable filenames of sequential files (for example video frame images).
     * @param number    the number where zeros will be prepended
     * @param max       the possible maximum of number
     * @return String of zeros (without the number itself)
     */
    public String addZeros(int number, int max) {
        StringBuffer zeros = new StringBuffer();

        for (int i = 0; i < Math.log10(max) && number < max / Math.pow(10.0, i); i++)
            zeros.append('0');
        return zeros.toString();
    }

    /**
     * Creates an image file of the input BufferedImage with the given frame number as part of its filename.
     * This method can be used to create images of video frames. These files are stored to /tmp/output/.
     * 
     * @param image     the BufferedImage to output
     * @param frame_no  the frame number to be included in the output filename.
     * @throws IOException May be thrown by the underlying <code>ImageIO.createImageOutputStream</code>.
     */
    private void createFrameImageFile(BufferedImage image, int frame_no) throws IOException {
        Iterator<ImageWriter> writers = ImageIO
                .getImageWritersByFormatName("jpeg");
        if (!writers.hasNext())
            throw new IllegalStateException(" no jpeg writer found");
        ImageWriter writer = (ImageWriter) writers.next();
        ImageWriteParam param = writer.getDefaultWriteParam();

        File outputFile = new File("/tmp/output/");
        // Uncomment this line (and comment the above one) if you want to create files on a per-task basis.
	    // File outputFile = new File( "/tmp/output/attempt_" + this.attemptID + "/" );

        outputFile.mkdirs();
        outputFile = new File(outputFile.getPath() + "/frame_"
                + addZeros(frame_no, 1000000) + frame_no + ".jpg");
        LOG.debug("outputFIle = " + outputFile);

        ImageOutputStream ios = ImageIO.createImageOutputStream(outputFile);
        writer.setOutput(ios);
        writer.write(null, new IIOImage(image, null, null), param);
        ios.flush();
        ios.close();
    }

    @Override
    public void setup(Context context) {
    }

    @Override
    public void map(IntWritable key, AVPacket value, Context context)
            throws IOException, InterruptedException {

        LOG.debug("Mapping");
        if (value != null && value.getStreamType() == AVPacket.StreamType.VIDEO ) {

            Configuration conf = context.getConfiguration();
            int delay;
            try {
                delay = Integer.parseInt(conf.get("video_batch.map.delay"));
            } catch (Exception e) {
                delay = 0;
            }

            //dummyDoSomethingForXMilliSeconds(delay);

            createFrameImageFile( value.getBufferedImage(), Integer.parseInt( key.toString()));
            
            int jobId = context.getJobID().getId();
            BufferedImage image = value.getBufferedImage();
            int color = image.getRGB(image.getHeight()/2, image.getWidth()/2);
            Color ccolor = new Color(color);
            RGBTable rgbTable = RGBTable.getInstance();
            rgbTable.putRGB("job-"+jobId, ccolor.getRed(), ccolor.getGreen(), ccolor.getBlue(), ccolor.getAlpha());

            //color == 0xAARRGGBB
            //int r = (argb)&0xFF;         1010 1101 1110 0111 1010 1101 1110 0111
            //int g = (argb>>8)&0xFF;      1111 1111 1010 1101 1110 0111 1010 1101 
            //int b = (argb>>16)&0xFF;     1111 1111 1111 1111 1010 1101 1110 0111 
            //int a = (argb>>24)&0xFF;     1111 1111 1111 1111 1111 1111 1010 1101 
            //          
            //System.out.println(" color: "+ Integer.toBinaryString(color));
            //System.out.println("  c>>8: "+ Integer.toBinaryString(color>>8));
            //System.out.println(" c>>16: "+ Integer.toBinaryString(color>>16));
            //System.out.println(" c>>24: "+ Integer.toBinaryString(color>>24));
            //System.out.println("RGBA: "+((color)&0xFF)+" "+((color>>8)&0xFF)+" "+((color>>16)&0xFF)+" "+((color>>24)&0xFF));
            //System.out.println("RGBA: "+ccolor.getBlue()+" "+ccolor.getGreen()+" "+ ccolor.getRed()+" "+ccolor.getAlpha());            

            LOG.info("'Mapping' frame " + key);
            
	        if (key == null)
	            return;

	        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

	        context.write(new Text(filename), new IntWritable(Integer.parseInt(key.toString())));
        }
    }


}
