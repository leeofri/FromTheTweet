package solution.tfidf;

import java.io.IOException;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/**
 * LineIndexMapper Maps each observed word in a line to a (filename@offset) string.
 */
public class WordCountsEachDocsMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    public WordCountsEachDocsMapper() {
    }
 

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	String delimiter = " +";
    	String splitValue = value.toString();
    	try
        {
	    	String[] wordAndDocCounter = splitValue.split("\t");
	    	
	    	delimiter = "@";
	    	splitValue = wordAndDocCounter[0];
	        String[] wordAndDoc = wordAndDocCounter[0].split("@");
	        
	        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
        }
        catch (ArrayIndexOutOfBoundsException IOB)
        {
        	System.out.println("INFO-ERROR: (WordCountsEachDocsMapper) IndexOutOfBoundsException. when try to split (delimiter '"+ delimiter + "') value: " + splitValue + " Massage: " + IOB.getMessage());
        }
    }
}


