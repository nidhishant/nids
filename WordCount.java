package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;

public class WordCount {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "“”\"[ .,;:!?-(){]");
			while (tokenizer.hasMoreTokens()) {
				String my_word = tokenizer.nextToken();
				word.set(my_word.toLowerCase());
				String output_size = "";
				for (int i = 0; i < my_word.length(); i++) {
					if (my_word.charAt(i) == 'a' || my_word.charAt(i) == 'e' || my_word.charAt(i) == 'i'
							|| my_word.charAt(i) == 'o' || my_word.charAt(i) == 'u' || my_word.charAt(i) == 'A'
							|| my_word.charAt(i) == 'E' || my_word.charAt(i) == 'I' || my_word.charAt(i) == 'O'
							|| my_word.charAt(i) == 'U') {
						output_size = output_size + "1";
					}
				}
				System.out.println(word + "  : " + output_size);
				output.collect((Text) word, new Text(output_size));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int size = 0;
			Text text = null;
			while (values.hasNext()) {
				text = values.next();
				// size += values.next().getLength();
				output.collect(key, new Text(String.valueOf(text.getLength())));
				System.out.println(key + "    :   " + String.valueOf(text.getLength()));
			}

		}
	}

	public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text arg0, Iterator<Text> arg1, OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			Text value = null;
			while (arg1.hasNext()) {
				value = arg1.next();

			}
			arg2.collect(arg0, value);
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		Text sample = new Text("1111");
		System.out.println(String.valueOf(sample.getLength()));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce2.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
