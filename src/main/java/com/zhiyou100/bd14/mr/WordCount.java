package com.zhiyou100.bd14.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {

	// 定义map(map输入类型key value, map输出类型key value)
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private String[] infos;
		private Text oKey = new Text();
		private final IntWritable oValue = new IntWritable(1);

		@Override
		protected void map(
				LongWritable key, 
				Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//解析一行数据, 转换成一个单词组成的数据
			infos = value.toString().split("\\s");
			for (String i : infos) {
				//把单词形成的一个kv对发送给reducer(单词,1)
				oKey.set(i);
				context.write(oKey, oValue);
			}
		}
	}
	
	//定义reducer
	public static class WordCountReducer extends Reducer< Text, IntWritable, Text, IntWritable>{
		private int sum;
		private IntWritable oValue = new IntWritable(0);
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context content) throws IOException, InterruptedException {
			sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			//输出kv
			oValue.set(sum);
			content.write(key, oValue);
		}
	}
	public static void main(String[] args) throws Exception{
		//Conguration
		Configuration configuration = new Configuration();
		//Job
		Job job = Job.getInstance(configuration);
		job.setJarByClass(WordCount.class);
		job.setJobName("第一个mr作业: wordcount");
		
		//mr
		job.setMapperClass(WordCountMap.class);
		job.setReducerClass(WordCountReducer.class);
		
		//kv
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//from data
		Path inputPath = new Path("/README.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		Path outputPath = new Path("/dirFromJava");
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//begin
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
