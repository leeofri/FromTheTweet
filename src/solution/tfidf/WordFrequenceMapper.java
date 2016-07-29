package solution.tfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishMinimalStemmer;

public class WordFrequenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	 
    public WordFrequenceMapper() {
    }
 

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	// split he tweet
		String[] tweet = value.toString().split("~%");
		
		try 
		{
			// Get the text and candidate from the tweet line-
			Text candidate = new Text(tweet[0]);
			String tweetText = tweet[1];
			String tweetPublishDay =tweet[2];
			
			 // Compile all the words using regex
	        Pattern p = Pattern.compile("\\w+");
	        Matcher m = p.matcher(tweetText);
	        EnglishMinimalStemmer engStemmer = new EnglishMinimalStemmer();
	        
	      
	        // build the values and write <k,v> pairs through the context
	        StringBuilder valueBuilder = new StringBuilder();
	        while (m.find()) {
	            String matchedKey = m.group().toLowerCase();
	            matchedKey.replaceAll("\\<.*?\\>", "");
	            
	            // Stemmer 
	            int cut = engStemmer.stem(matchedKey.toCharArray(), matchedKey.length());
	            matchedKey = matchedKey.substring(0, cut);
	            
	            // remove names starting with non letters, digits, considered stopwords or containing other chars
	            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
	                    || EnglishAnalyzer.getDefaultStopSet().contains(matchedKey) || matchedKey.contains("_")) {
	                continue;
	            }
	            
	            valueBuilder.append(matchedKey);
	            valueBuilder.append("@");
	            valueBuilder.append(tweetPublishDay);
	            
	            // emit the partial <k,v>
	            context.write(new Text(valueBuilder.toString()), new IntWritable(1));
	        }
		}
		catch (IndexOutOfBoundsException Iex)
		{
			System.out.println("INFO-ERROR: IndexOutOfBoundsException. tweet: " + value.toString());
		}
    }
}

