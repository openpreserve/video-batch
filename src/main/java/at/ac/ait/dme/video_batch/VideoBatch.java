package at.ac.ait.dme.video_batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
//import org.slf4j.Logger;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Base class for a video processing MapReduce job.
 * 
 * @author Matthias Rella, DME-AITma
 */
public abstract class VideoBatch extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(VideoBatch.class);

	/**
	 * Input file on hdfs
	 */
	protected Path file_on_hdfs;

	/**
	 * media framework for handling input media file
	 */
	protected AVMediaFramework mfw;

	/**
	 * Kinds of splitsize calculation. POWER_OF_2 sets split sizes of 2^x ANY
	 * sets split size to any positive value
	 */
	protected static enum Splitsize {
		POWER_OF_2, ANY
	};

	/**
	 * Type of splitsize calculation to use
	 */
	protected Splitsize splitsizeType = Splitsize.POWER_OF_2;

	/**
	 * Size of the InputSplits that will be distributed by MapReduce.
	 */
	protected long splitsize = 0;

	/**
	 * Parse arguments and perform setting of member variable if needed.
	 * 
	 * @param args
	 *            input arguments
	 * @throws IOException
	 */
	protected void parseArguments(String[] args) throws IOException {
	}

	/**
	 * Performs several preprocessing and configuration steps and finally runs
	 * MapReduce. The steps are:
	 * <ol>
	 * <li>Parse input directorclazzy and files and create uniform input file array
	 * 
	 * @param args
	 * @return
	 * @throws Exception
	 */

	public final int run(String[] args) throws Exception {

		//LogManager.getRootLogger().setLevel(Level.DEBUG);
		Enumeration e = LogManager.getCurrentLoggers();
		while (e.hasMoreElements()) {
			Logger l = (Logger) e.nextElement();
			System.out.println("L found:" + l.getName() + " debug: "
					+ l.isDebugEnabled());
		}
		Logger log4jLogger = LogManager.getLogger("at.ac.ait.dme.video_batch.VideoBatch");
		log4jLogger.setLevel(Level.DEBUG);
		

		/*
		 * String[] names = LogFactory.getFactory().getAttributeNames();
		 * for(String name : names) { Object value =
		 * LogFactory.getFactory().getAttribute(name);
		 * System.out.println("LOG: "+name+"="+value); }
		 */

		if (args.length < 1)
			throw new IllegalArgumentException("Name of input file missing");

		Configuration conf = getConf();

		FileSystem fs = FileSystem.get(conf);

		System.out.println("name: " + fs.getName() + " scheme: "
				+ fs.getScheme() + "working dir: " + fs.getWorkingDirectory());

		for (int i = 0; i < args.length; i++) {
			System.out.println("args " + i + " = " + args[i]);
		}

		System.out.println("Logging - is debug enabled: "
				+ LOG.isDebugEnabled());

		/*
		 * FileStatus[] status = fs.listStatus(new Path(
		 * "hdfs://localhost:8020/user/rainer")); // you need to pass in your
		 * hdfs // path
		 * 
		 * Path[] paths = FileUtil.stat2Paths(status);
		 * System.out.println("***** Contents of the Directory *****"); for(Path
		 * path : paths) { System.out.println(path); }
		 */

		file_on_hdfs = new Path(args[0]);

		if (!fs.exists(file_on_hdfs))
			throw new IllegalArgumentException(
					"Input file does not exist on hdfs");

		// prevent parsing the -libjars option
		if (args.length > 1 && !args[1].startsWith("-")) {
			splitsize = Long.parseLong(args[1]);
			LOG.info("Splitsize set to: "+args[1]);
		}

		parseArguments(args);

		// show block locations of file
		if (LOG.isDebugEnabled()) {
			FileStatus fstat = fs.getFileStatus(file_on_hdfs);
			BlockLocation[] locations = fs.getFileBlockLocations(fstat, 0,
					fstat.getLen());
			LOG.debug("Block Locations of " + file_on_hdfs.toString() + ":");
			for (BlockLocation loc : locations)
				LOG.debug(loc);
		}

		mfw = getMediaFramework();
		mfw.init(file_on_hdfs.makeQualified(fs).toUri());

		TreeMap<Long, Long> indices = mfw.getIndices(true);

		File index_file = serializeIndices(indices);

		String index_file_path = fs.getHomeDirectory().toString();
		String index_file_prefix = "cache_file_";

		conf.set("video_batch.index_file_path", index_file_path);
		conf.set("video_batch.index_file_prefix", index_file_prefix);

		String dest_cache_filename = index_file_path + Path.SEPARATOR
				+ index_file_prefix + file_on_hdfs.getName();

		// add index_file to distributed cache
		// job.addCacheFile(index_file_uri);
		// hadoop 0.20 way:
		// copy Index file to HDFS and store filename in conf property
		// DistributedCache is not used because it does not work in
		// RecordReader

		// does not work:
		// DistributedCache.addCacheFile(new URI( dest_cache_filename ), conf);

		fs.copyFromLocalFile(false, true,
				new Path(index_file.getAbsolutePath()), new Path(
						dest_cache_filename));

		conf.set("video_batch.codec", mfw.getCodecPackageName());

		configure(conf);

		// conf.set("video_batch.stream_encodings", streamEncodings.toString());
		Job job = new Job(conf);

		setupJob(job);
		// TODO: check before starting hadoop-process:
		// * Inputfile contains video stream(s)
		// * MediaFramework can decode video
		// * and other stuff which is done in demo DecodeAndPlayVideo.java

		if (getSplitsize() > 0)
			FileInputFormat.setMaxInputSplitSize(job, getSplitsize());

		FileInputFormat.setInputPaths(job, file_on_hdfs);
		FileOutputFormat.setOutputPath(job, new Path("output"));

		job.waitForCompletion(true);
		return 0;
	}

	/**
	 * Gets splitsize of InputSplits for this MapReduce job. It either returns
	 * the splitsize set by input arguments in main method or calculates it by
	 * size and length of GOP of the input media file. Splitsize may affect
	 * performance of the job.
	 * 
	 * @return long the splitsize value
	 */
	protected long getSplitsize() {

		// splitsize may be > 0 if set by input parameter
		if (splitsize > 0)
			return splitsize;

		// get average gop size in bytes
		long avg_gop_size = mfw.getAvgGOPSize();
		LOG.info("Mean GOP size in bytes: " + avg_gop_size);

		// get average gop length in bytes
		long avg_gop_length = mfw.getAvgGOPLength();
		LOG.info("Mean frames per GOP: " + avg_gop_length);

		// calculate optimal inputsplit size

		// minimum number of frames one node should process (should take at
		// least 1
		// min)
		int min_work_frames = 60000;

		// if split size should be a power of 2:
		switch (splitsizeType) {
		case ANY:
			if (avg_gop_length > 0)
				splitsize = (int) (avg_gop_size * Math.ceil(min_work_frames
						/ (double) avg_gop_length));
			break;

		case POWER_OF_2:
		default:
			int e = 20; // start at 1 MB
			while ((splitsize = (long) Math.pow(2, e)) <= avg_gop_size
					* Math.ceil(min_work_frames / (double) avg_gop_length))
				e++;
			break;
		}

		return splitsize;
	}

	/**
	 * Serialization method used for Hadoop v.0.20.2 where indices need to be
	 * stored to an hdfs file.
	 * 
	 * @param indices
	 *            TreeMap of indices
	 * @return File local (temporary) file containing serialized indices
	 * @throws IOException
	 *             matters of file operations
	 */
	protected File serializeIndices(TreeMap<Long, Long> indices)
			throws IOException {
		File index_file = File.createTempFile("indices_", ".tmp");

		if (LOG.isDebugEnabled()) {
			String output = "All Indices: Size " + indices.size() + " sorted:";
			Iterator<Entry<Long, Long>> it = indices.entrySet().iterator();
			while (it.hasNext())
				output += it.next() + "\n";
			//LOG.debug(output);
		}

		// serialize indices-list into file

		FileOutputStream fos = new FileOutputStream(index_file);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(indices);

		oos.flush();
		oos.close();

		fos.flush();
		fos.close();

		return index_file;
	}

	/**
	 * Quasi factory method to get a concrete MediaFramework.
	 * 
	 * @return MediaFramework framework to use in this job
	 */
	abstract protected AVMediaFramework getMediaFramework();

	/**
	 * Specifies job parameters.
	 * 
	 * @param job
	 *            MapReduce job to be setup
	 */
	abstract protected void setupJob(Job job);

	/**
	 * More configuration setting can be set via this method.
	 * 
	 * @param conf
	 *            Configuration instance of the job
	 */
	protected void configure(Configuration conf) {

	}

}
