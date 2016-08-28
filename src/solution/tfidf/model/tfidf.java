package solution.tfidf.model;

import javax.rmi.CORBA.Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.server.impl.cdi.Utils;

import solution.Globals;
import solution.tfidf.WordsInCorpusTFIDFMapper;
import solution.tfidf.WordsInCorpusTFIDFReducer;
 

public class tfidf extends Configured implements Tool {
 
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "tfidf");
        job.setJarByClass(tfidf.class);
        job.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job.setReducerClass(WordsInCorpusTFIDFReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        //Passing the total number of documents as the job name.
        job.setJobName(solution.Util.ReadingUserConfigFile(Globals.UserConfigFilePath()).get("numOfDays").toString());
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
