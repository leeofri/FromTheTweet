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
import org.apache.hadoop.util.ToolRunner;

import solution.tfidf.model.WordFrequence;
import solution.tfidf.model.tfidf;


public class FinalProj {


	public static void main(String[] args) throws Exception {
		
		// conf
		Configuration conf = new Configuration();

		String outPutPath = args[1];
		String inPutPath = args[0];
		Globals.setOutputFolder(outPutPath);
		
		// tfidf prerequisites 
		// word count
		String[] WordContPaths = {inPutPath, outPutPath+"/wordcountOutput"};
		
		int result = ToolRunner.run(new Configuration(), new WordFrequence(),WordContPaths );
	    
		if (result != 0 )
		{
			// exit with error
			System.exit(result);
		}
		
		// word count each doc
		String[] WordContEachDocPaths = {outPutPath+"/wordcountOutput",outPutPath+"/wordcountEachDocOutput"};
		result = ToolRunner.run(new Configuration(), new WordFrequence(), WordContEachDocPaths);
		
		if (result != 0 )
		{
			// exit with error
			System.exit(result);
		}
		
		
		// tfidf
		String[] tfidfPaths = {outPutPath+"/wordcountEachDocOutput",Globals.getOutputFolder().toString()};
		result = ToolRunner.run(new Configuration(), new tfidf(), tfidfPaths);
		
		if (result != 0 )
		{
			// exit with error
			System.exit(result);
		}
		
		//tweet semantic + tfidf analyze
	
		Job job = Job.getInstance(conf, "FinelProj");
		job.setJarByClass(FinalProj.class);
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(TweetReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		// add dictionary to cache 
		//DistributedCache.addLocalFiles(job.getConfiguration(), "/home/training/workspace/FromTheTweet/resources/wordDictionary.txt");
		DistributedCache.addCacheFile((new Path("/user/training/FromTheTweet/wordDictionary.txt").toUri()), job.getConfiguration());

		
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));
		FileInputFormat.addInputPath(job, new Path(inPutPath));
		
		// delete the privies run output
	    Util.IsDeleteUtputFolder(true, new Path(outPutPath));
		
	    // run canopy
		job.waitForCompletion(true);
		
		// Print the candidate counters
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