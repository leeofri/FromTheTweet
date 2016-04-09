package solution;

import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;

//calculate a new clustercenter for these vertices
public class TweetReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public static enum Counter {
		CONVERGED
	}

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

			// score summery counter
			double canidateSunScore = 0;
		
			// sum all the scores
			for (DoubleWritable score : values) {
				canidateSunScore =+ score.get();
			}
			
			context.write(key,new DoubleWritable(canidateSunScore));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);

	}
}
