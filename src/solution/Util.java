package solution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;


public class Util {

	public static Tweet GetTweetFromLine(Text value) {
		
		// split the the line to name|days..
		String[] data = value.toString().split("\\|");

		// create the 2D array
		DoubleWritable[][] tmp2DArray = new DoubleWritable[data.length - 1][];

		// Run on all days the first cell is the stack name
		for (int day = 1; day < data.length; day++) {
			String[] singleDaypParametrs = data[day].split(" ");
			tmp2DArray[day - 1] = new DoubleWritable[singleDaypParametrs.length];
			for (int paramter = 0; paramter < singleDaypParametrs.length; paramter++) {
				tmp2DArray[day - 1][paramter] = new DoubleWritable(
						Double.parseDouble(singleDaypParametrs[paramter]));
			}
		}

		return null;
	}

	public static int numberOfRowsInSeqFile(Path path, Configuration conf) throws IOException, InstantiationException, IllegalAccessException {

		Reader reader = null;

		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path));
		} catch (Exception e) {
			throw new IOException(e);
		}

		Writable key = (Writable) reader.getKeyClass().newInstance();
		Writable val = (Writable) reader.getValueClass().newInstance();

		int count = 0;
		while (reader.next(key, val)) {
			count++;
		}
		reader.close();
		return count;
	}
	
	public static void writeFileToHDFS (Configuration conf, String path, String Contant, Boolean ifAppend)
	{
		try{
            Path pt=new Path(path);
            FileSystem fs = FileSystem.get(conf);
            
            if (!ifAppend)
            {
            	fs.delete(pt,true);
            }
            
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));;
            br.write(Contant);
            br.close();
		}
		catch(Exception e){
            System.out.println("File not found");
		}
	}
	
	public static String readFileToHDFS (Configuration conf, String path)
	{
		String Content = new String();
		try{
			Path pt=new Path(path);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            //String line;
            Content=br.readLine();
		}
		catch(Exception e){
            System.out.println("File not found");
		}
		
		return Content;
	}
	
	public static void IsDeleteUtputFolder(Boolean indicate,Path path) {
		if (indicate) {
			// Deleting the output folder
			try {
				File outputFolder = new File(path.toString());
				FileUtils.deleteDirectory(outputFolder);
			} 
			catch (Exception e) {
				
				e.printStackTrace();
			}
	
			// Deleting the canopy seq file
			try {
				File outputFolder = new File("data/SequenceFile.canopyCenters");
				outputFolder.delete();
			} 
			catch (Exception e) {

				e.printStackTrace();
			}
		}
	}

	public static Map<String,String> ReadingUserConfigFile(Path path) throws IOException {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line;
			
			// the return map
			Map<String,String> map = new HashMap<String, String>();
			
			while ((line = br.readLine()) != null)
			{
				String[] values = line.split(" ");
				map.put(values[0], values[1]);
				
			}
			
			return map;

		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new IOException(e);

		}
	}
}
