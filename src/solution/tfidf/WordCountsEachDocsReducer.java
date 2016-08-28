package solution.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
 * WordCountsForDocsReducer counts the number of documents in the
 */
public class WordCountsEachDocsReducer extends Reducer<Text, Text, Text, Text> {
 
    public WordCountsEachDocsReducer() {
    }
 

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumOfWordsInDocument = 0;
        Map<String, Integer> tempCounter = new HashMap<String, Integer>();
        
      
	        for (Text val : values) {
	        	  	try
	        	  	{
			            String[] wordCounter = val.toString().split("=");
			            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
			            sumOfWordsInDocument += Integer.parseInt(wordCounter[1]);
			        }
		            catch (IndexOutOfBoundsException iuo)
		            {
		            	System.out.println("INFO-ERROR: IndexOutOfBoundsException. when try to split (delimiter '=') value: " + val.toString());
		            }
	        }
	        for (String wordKey : tempCounter.keySet()) {
	            context.write(new Text(wordKey + "@" + key.toString()), new Text(tempCounter.get(wordKey) + "/"
	                    + sumOfWordsInDocument));
	        }
     }
}