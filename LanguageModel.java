
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;
		// get the threashold parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 0);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t"); //MapReduce use \t to connect key & value of each intermediate result
			// test illegal intermediate result
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			// ignore phrases with low frequency when building languange model
			if(count < threashold) {
				return;
			}
			// output for 1Gram (word): (WDCNT, word = count)
			String tag = "WDCNT";
			if(words.length == 1) {
				context.write(new Text(tag), new Text(words[0] + "=" + count));
			} else {
				// outputKey: every word except the last
			    StringBuilder starting = new StringBuilder();
			    for(int i = 0; i < words.length - 1; i++) { // stop at the one before the last word
				    starting.append(words[i]).append(" ");
			    }
			    String outputKey = starting.toString().trim();
				// outputValue: every word
				StringBuilder fullWord = new StringBuilder();
			    for(int i = 0; i < words.length; i++) {
				    fullWord.append(words[i]).append(" ");
			    }
			    String outputValue = fullWord.toString().trim();
			    // output key-value pair: starting_phrase, full_word = count
			    if(!((outputKey == null) || (outputKey.length() < 1))) {
				    context.write(new Text(outputKey), new Text(outputValue + "=" + count));
					//context.write(new Text(outputValue), new Text(outputValue + "=" + count));
			    }
			}  
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// (key, value) pair of the tree is (count, following word)
			// ordered in descending value of count
			//String starting_phrase = key.toString().trim();
			int totalCount = 0;
			TreeMap<Integer, List<String>> tm = new TreeMap<>(Collections.reverseOrder());
			for(Text val: values) {
				String curValue = val.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				totalCount += count;
			    if(tm.containsKey(count)) {
					tm.get(count).add(word);
				} else {
					List<String> list = new ArrayList<>();
				    list.add(word);
					tm.put(count, list);
				}
				
			}
			Iterator<Integer> iter = tm.keySet().iterator();
			while (iter.hasNext()) {
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				for (String curWord: words){
					// output to MySQL: starting phrase, following word, count
					double relative_freq = (double) keyCount/ (double) totalCount;
					context.write(new DBOutputWritable(key.toString(), curWord, relative_freq), NullWritable.get());
				}
			}
			
		}
	}
}
