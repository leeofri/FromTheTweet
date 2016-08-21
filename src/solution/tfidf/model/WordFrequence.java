package solution.tfidf.model;
import solution.Globals;
import solution.tfidf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordFrequence extends Configured implements Tool  {

 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence");
 
        job.setJarByClass(WordFrequence.class);
        job.setMapperClass(WordFrequenceMapper.class);
        job.setReducerClass(WordFrequenceReducer.class);
        job.setCombinerClass(WordFrequenceReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + Globals.WORD_FREQ_OUTPUT_FOLDER));
//        
        // for debug
        FileInputFormat.addInputPath(job, new Path("/home/training/FromTheTweet/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/training/FromTheTweet/output2"));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
