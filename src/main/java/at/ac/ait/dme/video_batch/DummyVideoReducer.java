package at.ac.ait.dme.video_batch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * DummyVideoReducer serves as a playground for testing and evaluation purposes.
 * E.g. it simply counts values.
 * 
 * @author Matthias Rella, DME-AIT
 */
public class DummyVideoReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // simply count values
        Iterator<Text> it = values.iterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            i++;
        }
        context.write(key, new Text(i + ""));

    }
}
