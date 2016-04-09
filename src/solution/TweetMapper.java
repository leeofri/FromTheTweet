package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

import solution.ThirdParty.StdRandom;

//first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class TweetMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// Getting all the paths
		Path[] paths = context.getLocalCacheFiles();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// split he tweet
		String[] tweet = value.toString().split("~%");
		
		// Get the text and candidate from the tweet line
		Text candidate = new Text(tweet[0]);
		
		// Get the text from the tweet TODO: add tzvika algorithm
		DoubleWritable score = new DoubleWritable(StdRandom.random());
		
		
		context.write(candidate, score);

	}

	private Text CreateKey(Text text, Text text2) {

		Text result = new Text(String.valueOf(text) + String.valueOf(text2));

		return result;
	}

}
