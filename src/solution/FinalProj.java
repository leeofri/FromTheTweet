package solution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class FinalProj {

	private static int usedKmeans = 0;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		//"..\\resources\\wordDictionary.txt"
	
		Job job = Job.getInstance(conf, "FinelProj.Canopy");
		job.setJarByClass(FinalProj.class);
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(TweetReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		// add dictionary to chach
		DistributedCache.addLocalFiles(job.getConfiguration(), "/home/training/workspace/FromTheTweet/resources/wordDictionary.txt");
		//DistributedCache.addCacheFile((new Path("../resources/wordDictionary.txt")).toUri(), job.getConfiguration());

		Path outPutPath = new Path("output");
		
		FileOutputFormat.setOutputPath(job, outPutPath);
		FileInputFormat.addInputPath(job, new Path("input"));
		
		// delete the privies run output
	    Util.IsDeleteUtputFolder(true, outPutPath);
		
	    // run canopy
		job.waitForCompletion(true);

}}