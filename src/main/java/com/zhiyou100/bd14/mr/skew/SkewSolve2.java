package com.zhiyou100.bd14.mr.skew;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.bd14.mr.skew.SkewSolve1.SkewSolve2Reduce;

public class SkewSolve2 {

	
	public static class SkewSolve2Map extends Mapper
	<LongWritable, Text, Text, IntWritable>{
		
		private String[] infos;
		
		private Text outKey = new Text();
		private final IntWritable outValue = new IntWritable(1);
		
		@Override
		protected void map(
				LongWritable key, Text value, 
				Mapper<
				LongWritable, Text, 
				Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			infos = value.toString().split("\\s");
			
			if(infos.length == 2){
				outKey.set(infos[0]);
				outValue.set(Integer.valueOf(infos[1]));
				context.write(outKey, outValue);
			}
		}
	}
	
	public static void main(String[] args) 
			throws 
			IllegalArgumentException, 
			IOException, 
			ClassNotFoundException, 
			InterruptedException{
		
		Configuration conf = 
				new Configuration();
		
		//进行压缩
//		conf.set("mapreduce.map.output.compress", "true");
		//压缩格式
//		conf.set("mapreduce.map.output.compress.codes", "GzipCodec");
		
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SkewSolve2.class);
		
		job.setMapperClass(SkewSolve2Map.class);
		job.setReducerClass(SkewSolve2Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/output/Skew/SkewSolve1"));
		Path outputDir = new Path("/user/output/Skew/SkewSolve2");
		outputDir.getFileSystem(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);;
		
	}
}
