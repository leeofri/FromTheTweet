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
 
    /**
     * @param key is the byte offset of the current line in the file;
     * @param value is the line from the file
     * @param output has the method "collect()" to output the key,value pair
     * @param reporter allows us to retrieve some information about the job (like the current filename)
     *
     *     PRE-CONDITION: marcello@book.txt  \t  3/1500
     *     POST-CONDITION: marcello, book.txt=3/1500
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndCounters = value.toString().split("\t");
        String[] wordAndDoc = wordAndCounters[0].split("@");                 //3/1500
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
    }
}