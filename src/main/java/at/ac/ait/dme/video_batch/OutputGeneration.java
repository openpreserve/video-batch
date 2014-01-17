package at.ac.ait.dme.video_batch;

import at.ac.ait.dme.video_batch.xuggle.XugglerMediaFramework;
import at.ac.ait.dme.video_batch.xuggle.XugglerPacket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

/**
 * Implementation of VideoBatch that generates video output.
 * The used AVPartitioner devides the key space (frame numbers) and delegates them
 * to 2 Reducers. These use AVOutputFormat to generate video data.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class OutputGeneration extends VideoBatch{

    private static final Log LOG = LogFactory.getLog(VideoBatch.class);

    /**
     * Checks for input file name and kicks off the job.
     * Splitsize is an optional parameter. Used to set split size to a certain value.
     * 
     * @param args &lt;name of file on hdfs&gt; [splitsize] 
     */
    public static void main(String[] args) {
        
        try {
            int res = ToolRunner.run(new Configuration(), new OutputGeneration(),
                    args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    protected void setupJob( Job job ) {
        job.setJobName("output-generation");
        job.setJarByClass(OutputGeneration.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(XugglerPacket.class);

        // Mapper or Reducer is not needed in this case.
        // Default ones are used by MapReduce.

        job.setPartitionerClass(AVPartitioner.class);

        // setting reduce tasks to 2 to have two partitioners. 
        // that setting should be tested with a video with more than 400 frames (see AVPartitioner)
        job.setNumReduceTasks(2);

        job.setInputFormatClass(AVInputFormat.class);
        job.setOutputFormatClass(AVOutputFormat.class);
    }

    protected AVMediaFramework getMediaFramework() {
        return new XugglerMediaFramework();
    }

}
