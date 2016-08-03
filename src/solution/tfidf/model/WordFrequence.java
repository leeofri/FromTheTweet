package solution.tfidf.model;
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

    //folder in the  where to put the data in hdfs when we're done
    private static final String OUTPUT_FOLDER = "1-word-freq";
 
    // where to read the data from.
    private static final String INPUT_PATH = "input";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence");
 
        job.setJarByClass(WordFrequence.class);
        job.setMapperClass(WordFrequenceMapper.class);
        job.setReducerClass(WordFrequenceReducer.class);
        job.setCombinerClass(WordFrequenceReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequence(), args);
        System.exit(res);
    }

}
