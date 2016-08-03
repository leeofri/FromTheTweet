package solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FinalProj {


	public static void main(String[] args) throws Exception {
		
		
		//
		// tf-idf part one :  Word Frequency in Doc
		//
		
		
		
		
		
		
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
		//DistributedCache.addLocalFiles(job.getConfiguration(), "/home/training/workspace/FromTheTweet/resources/wordDictionary.txt");
		DistributedCache.addCacheFile((new Path("/user/training/FromTheTweet/wordDictionary.txt").toUri()), job.getConfiguration());

		Path outPutPath = new Path(args[1]);
		
		FileOutputFormat.setOutputPath(job, outPutPath);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// delete the privies run output
	    Util.IsDeleteUtputFolder(true, outPutPath);
		
	    // run canopy
		job.waitForCompletion(true);
		
		// Print the candidate conters
		Counters counters = job.getCounters();

		System.out.println("Candidate conters:");
		
		Counter BS = counters.findCounter( Globals.candidateCounters.BS);
		System.out.println(BS.getDisplayName()+":"+BS.getValue());
		
		Counter HC = counters.findCounter( Globals.candidateCounters.HC);
		System.out.println(HC.getDisplayName()+":"+HC.getValue());
		
		Counter DT = counters.findCounter( Globals.candidateCounters.DT);
		System.out.println(DT.getDisplayName()+":"+DT.getValue());
		
		Counter JK = counters.findCounter( Globals.candidateCounters.JK);
		System.out.println(JK.getDisplayName()+":"+JK.getValue());
		
		Counter TC = counters.findCounter( Globals.candidateCounters.TC);
		System.out.println(TC.getDisplayName()+":"+TC.getValue());

}
	
	}