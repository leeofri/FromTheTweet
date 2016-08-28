package solution.tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import solution.Globals;
 
/**
 * WordsInCorpusTFIDFReducer calculates the number of documents in corpus that a given key occurs and the TF-IDF computation.
 * The total number of D is acquired from the job name<img width="16" height="16" class="wp-smiley emoji" draggable="false" alt=":)" src="https://s1.wp.com/wp-content/mu-plugins/wpcom-smileys/simple-smile.svg" style="height: 1em; max-height: 1em;"> It is a dirty hack, but the only way I could communicate the number from
 * the driver.
 */
public class WordsInCorpusTFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
 
    private static final DecimalFormat DF = new DecimalFormat("###.########");
 
    public WordsInCorpusTFIDFReducer() {
    }
 
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // get the number of documents indirectly from the file-system (stored in the job name on purpose)
        int numberOfDocumentsInCorpus = Integer.parseInt(context.getJobName());
        // total frequency of this word
        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] documentAndFrequencies = val.toString().split("=");
            numberOfDocumentsInCorpusWhereKeyAppears++;
            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
        }
        for (String document : tempFrequencies.keySet()) {
            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
 
            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                    / Double.valueOf(wordFrequenceAndTotalWords[1]));
 
            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
            double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;
 
            //given that log(10) = 0, just consider the term frequency in documents
            double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ?
                    tf : tf * Math.log10(idf);
 
            context.write(new Text(key + "@" + document), new DoubleWritable(tfIdf));
        }
    }
}