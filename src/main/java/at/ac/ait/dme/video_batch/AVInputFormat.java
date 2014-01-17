package at.ac.ait.dme.video_batch;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class AVInputFormat extends FileInputFormat<IntWritable, AVPacket> {

    @Override
    public RecordReader<IntWritable, AVPacket> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        return new AVPacketRecordReader();
    }

}
