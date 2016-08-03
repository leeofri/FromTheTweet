package solution.tfidf.model;
import solution.tfidf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class WordFrequence extends Configured implements  {

    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "1-word-freq";
 
    // where to read the data from.
    private static final String INPUT_PATH = "input";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Word Frequence");
 
        job.setJarByClass(WordFrequence.class);
        job.setMapperClass(WordFrequenceMapper.class);
        job.setReducerClass(WordFrequenceReducer.class);
        job.setCombinerClass(WordFrequenceReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequence(), args);
        System.exit(res);
    }

}
