package com.zhiyou100.bd14.mr.homeWork;

import java.io.IOException;
import java.util.Arrays;

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
import org.apache.hadoop.util.StringUtils;

public class TwoMapReduceHomeWork {

	public static int num;
	
	/*
	 * 1. 读取文件, 
	 * 2. 对文件进行分解, 
	 * 3. 输出单词, 1
	 */
	public static class TwoMapReduceHomeWorkMap
	extends Mapper<
	LongWritable, Text, 
	Text, IntWritable>{
		
		private String[] infos;
		
		private Text outKey = 
				new Text();
		private final IntWritable ONE =
				new IntWritable(1);
		
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println(num);
			num++;
		}



		@Override
		protected void map(
				LongWritable key, Text value, 
				Mapper<
				LongWritable, Text, 
				Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			infos = StringUtils.split(value.toString(),' ');
			
		
			
			for(String word : infos){
				outKey.set(word);
				context.write(outKey, ONE);
			}
		}
	}
	
	//combinner 进行单词计数
	public static class TwoMapReduceHomeWorkCombiner
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private int sum;
		
		private IntWritable outValue = 
				new IntWritable();

		@Override
		protected void reduce(
				Text key, Iterable<IntWritable> values,
				Reducer<
				Text, IntWritable, 
				Text, IntWritable>.Context context) throws IOException, InterruptedException {
			sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
		}
	}
	
	
	public static class TwoMapReduceHomeWorkReduce
	extends Reducer<
	Text, IntWritable, 
	Text, IntWritable>{
		
		private int sum;
		private IntWritable outValue = 
				new IntWritable();
		@Override
		protected void reduce(
				Text key, Iterable<IntWritable> values,
				Reducer<
				Text, IntWritable, 
				Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			sum = 0;
			for(IntWritable value : values){
				sum += value.get();
				outValue.set(sum);
			}
			context.write(key, outValue);
		}
	}
	
	
	public static void main(String[] args) 
			throws 
			IOException, 
			ClassNotFoundException, 
			InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "两个Map");
		job.setJarByClass(TwoMapReduceHomeWork.class);
		
		job.setMapperClass(TwoMapReduceHomeWorkMap.class);
		job.setCombinerClass(TwoMapReduceHomeWorkCombiner.class);
		job.setReducerClass(TwoMapReduceHomeWorkReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.setMaxInputSplitSize(job, 50L);
		
		
		FileInputFormat.addInputPath(job, new Path("/user/aaa.txt"));
		
		Path outputDir = new Path("/user/output/TwoMapReduceHomeWork");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
