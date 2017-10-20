package com.zhiyou100.bd14.mr.skew;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.bd14.mr.wordCount.WordCount;
import com.zhiyou100.bd14.mr.wordCount.WordCount.WordCountMap;
import com.zhiyou100.bd14.mr.wordCount.WordCount.WordCountReducer;

//倾斜key值 : a
public class SkewSolve1 {

	
	/*
	 * 1. 在map输出key值是判断是否是倾斜key, 
	 * 如果是就给他加上一个随机标识, 改变源数据内容, 这样相同的key
	 * 就会被发送到不同的reduce节点, 从而打散倾斜key值
	 * 不足的地方时在下一个mr富哦成中需要把这个随机标识给认为的去掉, 稍有不慎数据就会出错
	 */
/*	public static class SkewSolve1Map 
	extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text outKey = new Text();
		private final IntWritable ONE = new IntWritable(1);
		private String[] infos;
		
		private Random random = new Random();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			infos = value.toString().split("\\s");
			
			for(String word : infos){
				
				if(word.equals("a")){
					outKey.set(
							word+"_"+
								random.nextInt(context.getNumReduceTasks()));
				}else{
					outKey.set(word);
					context.write(outKey, ONE);
				}
			}
		}
	}*/
	
	//2. map不做改变, 通过该自定义partitioner来实现打散倾斜key值的功能
	public static class SkewSolve2Partitioner 
	extends
	Partitioner<Text, IntWritable>{
		
		private final String[] SKEW_KEYS = 
			{"a"};
		
		private Random random = 
				new Random();

		@Override
		public int getPartition(
				Text key, IntWritable value, 
				int numPartitions) {
			
			for(String skewKey : SKEW_KEYS){
				if(key.toString().equals(skewKey)){
					return random.nextInt(numPartitions);
				}
			}
			return ( key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
		}
		
	}
	
	public static class SkewSolve2Map extends Mapper
	<LongWritable, Text, Text, IntWritable>{
		
		private String[] infos;
		
		private Text outKey = new Text();
		private final IntWritable ONE = new IntWritable(1);
		
		private int keyValueNumbers = 0;
		
		@Override
		protected void map(
				LongWritable key, Text value, 
				Mapper<
				LongWritable, Text, 
				Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			infos = value.toString().split("\\s");
			
			keyValueNumbers = 0;
			
			for(String word : infos){
				
				keyValueNumbers ++;
				
				outKey.set(word);
				context.write(outKey, ONE);
			}
		}
	}
	
	
	
	public static class SkewSolve2Reduce 
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private int sum = 0;
		private IntWritable outValue = 
				new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
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
	
	
	
	public static void main(String[] args) 
			throws 
			IOException, 
			ClassNotFoundException, 
			InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(
				conf, 
				"解决数据倾斜(拆分成两个mr的第一个mr)");
		job.setJarByClass(SkewSolve1.class);
		
		job.setMapperClass(SkewSolve2Map.class);
		job.setPartitionerClass(SkewSolve2Partitioner.class);
		job.setReducerClass(SkewSolve2Reduce.class);
		
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inputPath = new Path("/user/aaa.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		Path outputPath = new Path("/user/output/Skew/SkewSolve1");
		outputPath.getFileSystem(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
