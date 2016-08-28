package solution;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

public class scoreDictionary {

	private Map<String, double[]> dictionary;
	private Map<String, Double> tfidfDictionary;
	private double maxVal;
	private double minVal;
	
	public scoreDictionary(String path,String tfidfDictionaryPath) throws IOException
	{
		this.dictionary = new HashMap<String, double[]>();
		
		this.tfidfDictionary = Util.ReadingMapMinMax(new Path(tfidfDictionaryPath));
		
		// put the max and min val
		this.maxVal = this.tfidfDictionary.get("@MAX").doubleValue();
		this.minVal = this.tfidfDictionary.get("@MIN").doubleValue();
		
		
		this.SentiWordNetDemoCode(path);
	}
	
	
	public void SentiWordNetDemoCode(String pathToSWN) throws IOException {

		BufferedReader csv = null;
		try {
			csv = new BufferedReader(new FileReader(pathToSWN));
			int lineNumber = 0;

			String line;
			while ((line = csv.readLine()) != null) {
				lineNumber++;

				// If it's a comment, skip this line.
				if (!line.trim().startsWith("#")) {
					// We use tab separation
					String[] data = line.split("\t");
					
					// Is it a valid line? Otherwise, through exception.
					if (data.length != 6) {
						throw new IllegalArgumentException(
								"Incorrect tabulation format in file, line: "
										+ lineNumber);
					}

					// Calculate synset score as score = PosS - NegS
					Double synsetScore = Double.parseDouble(data[2])
							- Double.parseDouble(data[3]);

					// Get all Synset terms
					String[] synTermsSplit = data[4].split(" ");

					// Go through all terms of current synset.
					for (String synTermSplit : synTermsSplit) {
						String word = synTermSplit.split("#")[0];
						if (!dictionary.containsKey(word)) {
							double[] val = new double[2];
							val[0] = synsetScore;
							val[1] = 1;
							dictionary.put(word, val);
						}
						else {
							double[] val = dictionary.get(word);
							val[0] += synsetScore;
							val[1]++;
							dictionary.put(word, val);
							
						}
					}
				}
			}
			for(Map.Entry<String, double[]> entry : dictionary.entrySet()) {
				double[] val = entry.getValue();
				double[] avg = new double[1];
				avg[0] = val[0] / val[1];
				entry.setValue(avg);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (csv != null) {
				csv.close();
			}
		}
	}
	
	public double extract(String word) {
		if (dictionary.containsKey(word))
			return dictionary.get(word)[0];
		else
			return -100;
	}
	
	public double extractTfidf(String word) {
		if (tfidfDictionary.containsKey(word))
			return ((tfidfDictionary.get(word).doubleValue() - minVal) / (maxVal - minVal));
		else
			return 1;
	}
	
	public double getSentenceGrade(String sentence) {
		double sum = 0, val = 0,tfidfVal = 0;
		int count = 0;
		String[] words = sentence.split("\\s+");
		

		for (int i = 0; i < words.length; i++) {
		    words[i] = words[i].replaceAll("[^\\w]", "");
		    words[i] = words[i].toLowerCase();
		    val = extract(words[i]);
		    if (val != -100) {
		    	sum += val * extractTfidf(words[i]);
		    	count++;
		    }
		}
		return sum / count;
	}

}
