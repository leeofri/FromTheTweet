package solution.tfidf;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
/**
 * WordsInCorpusTFIDFMapper implements the Job 3 specification for the TF-IDF algorithm
 */
public class WordsInCorpusTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    public WordsInCorpusTFIDFMapper() {
    }
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndCounters = value.toString().split("\t");
        String[] wordAndDoc = wordAndCounters[0].split("@");                 //3/1500
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
    }
}