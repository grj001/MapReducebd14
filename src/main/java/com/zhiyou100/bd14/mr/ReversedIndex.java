package com.zhiyou100.bd14.mr;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReversedIndex {
	private static String TEST = "out";
	
	public static class ReversedIndexMap extends Mapper<LongWritable, Text, Text, Text>{
		private String[] infos;
		
		private String filePath;
		private FileSplit fileSplit;
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			fileSplit = (FileSplit) context.getInputSplit();
			filePath = fileSplit.getPath().toString();
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("[\\s\"\\(\\)]"); //value是一行的文字
			if(infos != null && infos.length > 0){ //如果不是空行, 且长度大于0
				for(String word : infos){
					outputKey.set(word); //一行中的一个单词
					fileSplit = (FileSplit) context.getInputSplit(); //fileSplit
					filePath = fileSplit.getPath().toString(); //filePath
					outputValue.set(filePath); //outputValue = filePath
					context.write(outputKey, outputValue); //write = word filePath 
					System.out.println("map out:\t\t"+outputKey+"\t\t"+outputValue);
				}
			}
		}
	}
	
	public static class ReversedIndexCombinner extends Reducer<Text, Text, Text, Text>{
		private int wordCount;
		private Text outputValue = new Text();
		private String filePath;
		private boolean isGetFilePath;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			isGetFilePath = false;
			wordCount = 0;	//每读一个key, 进行一次清空
			//相同单词, 相同路径的进行计数
			//BIS		hdfs://master:9000/user/reversetext/README.txt[2]
			for(Text path : values){
				wordCount += 1; //每有一个key, 进行加
				if(!isGetFilePath){
					filePath = path.toString();
					isGetFilePath = true;
				}
			}
			outputValue.set(filePath + "["+wordCount+"]"); //filePath + wordCount
			context.write(key, outputValue); //向reducer里面输出 key:word outputValue:filePath
			System.out.println("Combinner out:\t\t"+key+"\t\t"+outputValue);
		}	
	}
	
	public static class ReversedIndexReducer extends Reducer<Text, Text, Text, Text>{
		//这里的变量只创建一次
		private StringBuffer valueStr;
		private Text outputValue = new Text();
		private boolean isInitlized = false;
		@Override
		//同一个key可以进入reduce, 需要设置路径和字出现的次数, 
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			valueStr = new StringBuffer();
			//相同单词, 不同路径, 路径拼接
			/*
			 * with	---
			 * hdfs://master:9000/user/reversetext/README.txt[1]---
			 * hdfs://master:9000/user/reversetext/reverse3.txt[2]---
			 * hdfs://master:9000/user/reversetext/reverse2.txt[1]---
			 * hdfs://master:9000/user/reversetext/LICENSE.txt[16]---
			 * hdfs://master:9000/user/reversetext/reverse4.txt[1]
			 */
			for(Text value : values){
				if(isInitlized){
					valueStr.append("---" + value.toString()); //第二次添加 filePath + wordCount
				}else{
					valueStr.append(value.toString()); //第一次添加
					isInitlized = true;
				}
 			}
			outputValue.set(valueStr.toString());
			context.write(key, outputValue);
			System.out.println("Reducer out:\t\t"+key+"\t"+outputValue);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReversedIndex.class);
		job.setJobName("倒排索引");
		
		job.setMapperClass(ReversedIndexMap.class);
		job.setCombinerClass(ReversedIndexCombinner.class);
		job.setReducerClass(ReversedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path inputPath = new Path("/user/reversetext");
		Path outputDir = new Path("/user/output/ReversedIndex");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
