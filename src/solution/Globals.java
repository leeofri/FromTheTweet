package solution;import java.sql.Ref;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Globals {
	
	 enum candidateCounters {
         HC,
         DT,
         BS,
         TC,
         JK
	 }

    //folder in the  where to put the data in hdfs when we're done
    public static final String WORD_FREQ_OUTPUT_FOLDER = "WORD_FREQ";
	    
	static String outputFolder = "./output";
	
	public static void updateCandidate (String candidateCode,int numOfTweets, Context context)
	{
		if (candidateCode == "DT") 
		{
			context.getCounter(candidateCounters.DT).increment(numOfTweets);
		}
		else if (candidateCode == "HC")
		{
			context.getCounter(candidateCounters.HC).increment(numOfTweets);
		}
		else if (candidateCode == "BS")
		{
			context.getCounter(candidateCounters.BS).increment(numOfTweets);
		}
		else if (candidateCode == "TC")
		{
			context.getCounter(candidateCounters.TC).increment(numOfTweets);
		}
		else if (candidateCode == "JK")
		{
			context.getCounter(candidateCounters.JK).increment(numOfTweets);
		}
		
	}
	
	public static Path getOutputFolder()
	{
		return new Path(outputFolder);
	}
	
	public static void setOutputFolder(String outputPath)
	{
		outputFolder = outputPath;
	}
	
	
	
	public static Path UserConfigFilePath()
	{
		return new Path("./data/userConfigFile.config");
	}
	
}
