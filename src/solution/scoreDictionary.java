package solution;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class scoreDictionary {

	private Map<String, double[]> dictionary;
	
	public scoreDictionary(String path) throws IOException
	{
		this.dictionary = new HashMap<String, double[]>();
		
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
	
	public double getSentenceGrade(String sentence) {
		double sum = 0, val = 0;
		int count = 0;
		String[] words = sentence.split("\\s+");
		for (int i = 0; i < words.length; i++) {
		    words[i] = words[i].replaceAll("[^\\w]", "");
		    words[i] = words[i].toLowerCase();
		    val = extract(words[i]);
		    if (val != -100) {
		    	sum += val;
		    	count++;
		    }
		}
		return sum / count;
	}

//	public void main(String [] args) throws IOException {
//		
//		SentiWordNetDemoCode(path);
////		System.out.println("hi");
//		System.out.println("good "+extract("good"));
//		System.out.println("bad "+extract("bad"));
//		System.out.println("blue "+extract("blue"));
//		
//		System.out.println("old "+extract("old"));
//		System.out.println("happy "+extract("happy"));
//		System.out.println("ugly "+extract("ugly"));
//		System.out.println("retarded "+extract("retarded"));
//		System.out.println("strong "+extract("strong"));
//		
//		String sen1 = "I believe in trump, he is the future";
//		String sen2 = "Trump is such a loser, what a dick";
//		String sen3 = "I don't, maybe clinton is the right choice after all";
//		String sen4 = "This clinton woman is a gonna destroy this country";
//		String sen5 = "this IS! just a 1234 test! why??? not! #hash1 #very_long_hash :) :P XD";
//		System.out.println(getSentenceGrade(sen1));
//		System.out.println(getSentenceGrade(sen2));
//		System.out.println(getSentenceGrade(sen3));
//		System.out.println(getSentenceGrade(sen4));
//		System.out.println(getSentenceGrade(sen5));
//	}
}
