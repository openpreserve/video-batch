package at.ac.ait.dme.video_batch;

import at.ac.ait.dme.video_batch.xuggle.XugglerMediaFramework;
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


/**
 * Simple VideoBatch implementation to run MapReduce job primarily for evaluation and testing purposes.
 * An artificial delay is inserted into the MapTask by using DummyVideoMapper.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class SimpleVideoBatch extends VideoBatch {

    private static final Log LOG = LogFactory.getLog(VideoBatch.class);

    /**
     * Artificial delay to be inserted into the map task.
     */
    int delay = 0;

    /**
     * Checks for input file name and kicks off the job.
     * Splitsize is an optional parameter. Used to set split size to a certain value.
     * 
     * @param args &lt;name of file on hdfs&gt; [splitsize] 
     */
    public static void main(String[] args) {
        
        try {
            int res = ToolRunner.run(new Configuration(), new SimpleVideoBatch(),
                    args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void parseArguments(String[] args) {
    	if (args.length > 3) {
        	try {
            delay = Integer.parseInt(args[3]);
            LOG.info("Setting mapping delay: " + delay);
        	} catch(Exception e) {
        		LOG.info("No mapping delay set");
        	}
        }
    }

    @Override
    protected void configure(Configuration conf) {

        // clean up temporary output directory
        File tmp_output = new File("/tmp/output/");
        if (tmp_output.isDirectory()) {
            File[] files = tmp_output.listFiles();
            for (int f = 0; f < files.length; f++)
                files[f].delete();
        }

        // set mapping delay
        conf.set("video_batch.map.delay", delay + "");

        LOG.info("hadoop.native.lib = "
                + System.getProperty("hadoop.native.lib"));
        LOG.info("java.library.path = "
                + System.getProperty("java.library.path"));

    }

    protected AVMediaFramework getMediaFramework() {
        return new XugglerMediaFramework();
    }

    @Override
    protected void setupJob(Job job) {
        job.setJobName("simple-videobatch");
        job.setJarByClass(SimpleVideoBatch.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(DummyVideoMapper.class);

        job.setReducerClass(DummyVideoReducer.class);

        job.setInputFormatClass(AVInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }
}
