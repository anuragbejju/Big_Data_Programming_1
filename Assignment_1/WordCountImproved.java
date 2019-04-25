/*

Professional Masters in Big Data Program - Simon Fraser University

Assignment 1 (Question 1 - WordCountImproved.java)

Submission Date: 11th September 2018
Name: Anurag Bejju
Student ID: 301369375
Professor Name: Gregory Baker

*/


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer; //Import Predefined LongSumReducer Class

public class WordCountImproved extends Configured implements Tool {
//WordCountMapper Class
	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
		) throws IOException, InterruptedException {
			String input_line = value.toString();   //Convert the value to string in order to use the split command
			Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+"); //Initializing the pattern based on the requirement
			String[] word_split = word_sep.split(input_line); // Split the line based on the predefined pattern
			for (String word_indiv:word_split)
			{
				String temp_word = word_indiv.toLowerCase(); //Convert each word in lower case
				if(temp_word.length()>0) //Check for empty strings
				{
					word.set(temp_word); //If not empty, set word variable of type text
					context.write(word , one); //Populate context with the word and count 1
				}
			}

		}
	}



	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountImproved.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
