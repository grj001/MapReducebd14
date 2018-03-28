package com.zhiyou100.bd14.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.zhiyou100.bd14.mr.IPLoginAndNewTweetSecondarySort.LoginAndTweetTimes;

public class IPLoginAndNewTweet {
	
	
	
	
	
	
	//map
	public static class IPLoginAndNewTweetMap extends Mapper<LongWritable, Text, Text, Text>{
		private String[] infos;
		private Text oKey = new Text();
		private Text oValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//System.out.println(key.toString()+"--"+value.toString());
			infos = value.toString().split("\\s");
			//ip
			oKey.set(infos[2]);
			//Status
			oValue.set(infos[1]);
			//System.out.println(oKey.toString()+"--"+oValue.toString());
			context.write(oKey, oValue);
		}
	}
	
	
	
	public static class IPLoginAndNewTweetReduce extends Reducer<Text, Text, Text, LoginAndTweetTimes> {
		private Text oKey = new Text();
		private LoginAndTweetTimes oValue = new LoginAndTweetTimes();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, LoginAndTweetTimes>.Context context)
				throws IOException, InterruptedException {
			Integer tweetTimes = 0;
			Integer loginTimes = 0;
			
			
			for(Text value : values){
				//System.out.println(key.toString()+"--"+value.toString());
				if(value.toString().equals("new_tweet")){
					tweetTimes++;
				}else if(value.toString().equals("login")){
					loginTimes++;
				}
			}
			
			oKey.set(key);
			
			oValue.setLoginTimes(loginTimes);
			oValue.setTweetTimes(tweetTimes);
			context.write(oKey, oValue);
			//System.out.println(key+"--"+lt);
		}
		
	}
	
	
	
	//main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance();
		job.setJarByClass(IPLoginAndNewTweet.class);
		job.setJobName("计算登录次数, 推特次数");
		
		job.setMapperClass(IPLoginAndNewTweetMap.class);
		job.setReducerClass(IPLoginAndNewTweetReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LoginAndTweetTimes.class);
		
		Path inputPath = new Path("/user/user-logs-large.txt");
		Path outputDir = new Path("/IPLoginAndNewTweet");
		
		outputDir.getFileSystem(conf).delete(outputDir,true);
		
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
