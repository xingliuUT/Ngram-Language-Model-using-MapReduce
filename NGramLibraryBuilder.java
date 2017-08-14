
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			line = line.trim().toLowerCase(); //convert all text to lower case
			line = line.replaceAll("[^a-z]", " ");  //replace all non-letter symbols to blank space
			
			String[] words = line.split("\\s+"); //split by ' '
			
			// start from 1Gram (word)
			if(words.length < 0) {
				return;
			}
			
			StringBuilder sb;
			// i scans through every word
			for(int i = 0; i < words.length; i++) {
				sb = new StringBuilder();
				//j = 0, ..., Ngram is the number of words after the initial word
				for(int j = 0; i + j < words.length && j < noGram; j++) { 
					sb.append(" ");
					sb.append(words[i + j]);
					// output key-value pair: (phrase, 1)
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values) {
				sum += value.get();
			}
			// output key-value pair: (phrase, count)
			context.write(key, new IntWritable(sum));
		}
	}

}