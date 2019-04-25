/*

Professional Masters in Big Data Program - Simon Fraser University

Assignment 2 (Question 1 - WikipediaPopular.java)

Submission Date: 14th September 2018
Name: Anurag Bejju
Student ID: 301369375
Professor Name: Gregory Baker

*/

/*
Code Synopsis

[Mapper] -> [Combiner] -> [Reducer]

WikipediaPopularMapperClass - Parses the raw data and remove redundant information.
This leaves us with the date associated with its request count in LongWritable format.
This also satifies the program conditions like only English Wikipedia pages, no Main_Page and special pages to be counted.

WikipediaPopularCombiner- This performs same function as WikipediaPopularReducerClass, so did not explicitly create it.

WikipediaPopularReducerClass - This further reduces the data and provide result with the hourly timestamp and the maximum request count in that hour.

*/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular  extends Configured implements Tool {

	//WikipediaPopularMapperClass

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
		) throws IOException, InterruptedException {
			String line = value.toString();

			LongWritable val = new LongWritable();
			String[] input_string = line.split("\\n");
			for(int i=0;i<input_string.length; i++){
				String[] splited = (input_string[i]).split("\\s+");
				if (((splited[1]).equals("en")) && (!((splited[2]).equals("Main_Page")))&& (!((splited[2]).startsWith("Special:"))))
				{
					long j = Long.parseLong(splited[3]);
					val.set(j);
					context.write(new Text(splited[0]), val);  // create hourly timestamp and request count
				}
			}
		}
	}

	//WikipediaPopularReducerClass Class
	public static class WikipediaPopularReducer
	extends Reducer<Text , LongWritable, Text , LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values,
		Context context
		) throws IOException, InterruptedException {
			System.out.println("Reducer is started");
			int i = 0;
			long max = 0;
			LongWritable max_value = new LongWritable();
			for (LongWritable val : values) {
				//code for finding the max value

				if (i == 0)
				{
					max = val.get();
					i = i +1;
				}
				else if (max < val.get())
				{
					max = val.get();
				}
			}
			max_value.set(max);
			context.write(key, max_value);
			System.out.println("Reducer is done");
		}
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Wikipedia Popular");
		job.setJarByClass(WikipediaPopular.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setCombinerClass(WikipediaPopularReducer.class);
		job.setReducerClass(WikipediaPopularReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
