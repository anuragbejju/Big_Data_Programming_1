/*

Professional Masters in Big Data Program - Simon Fraser University

Assignment 2 (Question 2 - RedditAverage.java)

Submission Date: 11th September 2018
Name: Anurag Bejju
Student ID: 301369375
Professor Name: Gregory Baker

*/

/*
Code Synopsis

[Mapper] -> [Combiner] -> [Reducer]

RedditMapperClass - Parses the JSON raw data and remove redundant information.
										This leaves us with the subreddit name associated with its [frequency , score] in longPairWritable format.

RedditCombinerClass- This combines all the data coming from mapper into one file and provides a [frequency_cummilative, score_cummilative] in longPairWritable format.

RedditReducerClass - This further reduces the data and provide result with the subreddit name and its associated average score.

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
import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

	//RedditMapperClass

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
		) throws IOException, InterruptedException {
			String subreddit_val;
			int score_val;
			String line = value.toString();
			String[] input_string = line.split("\\n");
			for(int i=0;i<input_string.length; i++){

				JSONObject obj = new JSONObject(input_string[i]); //get one complete json object
				LongPairWritable pair = new LongPairWritable();
				subreddit_val = obj.getString("subreddit"); //get associated values
				score_val = obj.getInt("score");
				pair.set(1, new Integer(score_val));
				context.write(new Text(subreddit_val), pair);  // create subreddit_name and <frequency, score>
			}


		}
	}

	//RedditCombiner Class
	public static class RedditCombiner
	extends Reducer<Text , LongPairWritable, Text , LongPairWritable> {
		private LongPairWritable result = new LongPairWritable();
		public void reduce(Text key, Iterable<LongPairWritable> values,
		Context context
		) throws IOException, InterruptedException {
			System.out.println("Combiner is started");
			long freq_combiner = 0;
			long score_combiner = 0;
			for (LongPairWritable val : values) {
				freq_combiner += val.get_0();  //adding up the frequency of each word in each file
				score_combiner += val.get_1(); //adding up the score of each word in each file
			}
			result.set(freq_combiner,score_combiner);
			context.write(key, result);
			System.out.println("Combiner is done");
		}
	}

	//RedditReducerClass

	public static class RedditReducer
	extends Reducer<Text , LongPairWritable, Text , DoubleWritable> {

		private DoubleWritable result_final = new DoubleWritable();

		public void reduce(Text key,  Iterable<LongPairWritable> values,
		Context context
		) throws IOException, InterruptedException {
			System.out.println("Reducer is started");
			double freq_reducer = 0.0;
			double score_reducer = 0.0;
			for (LongPairWritable val : values) {
				freq_reducer += val.get_0();   //adding up the frequency of each word in the combined file
				score_reducer += val.get_1(); //adding up the score of each word in the combined file
			}
			double avg_score_value =  score_reducer/ freq_reducer; //finding the average_score for each word
			result_final.set(avg_score_value);
			context.write(key, result_final);
			System.out.println("Reducer is done");
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Reddit Average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setCombinerClass(RedditCombiner.class);
		job.setReducerClass(RedditReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
