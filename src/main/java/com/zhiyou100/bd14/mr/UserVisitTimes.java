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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

//计算出访问该系统的每一个用户的用户名
//去重
public class UserVisitTimes {
	
	
	
	//从文件中抽取用户名, 并将其作为key 发送到reducer节点, value与节点无关
	public static class DesDumolicateMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private String[] infos;
		private IntWritable oValue = new IntWritable(1);
		private Text oKey = new Text();
		
		
		@Override
		protected void map(
				//map中的kv对
				LongWritable key, 
				Text value,
				//reduce中的kv对
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			infos = value.toString().split("\\s");
			//System.out.println(Arrays.toString(infos));
			
			if(infos[1].equals("login")){
				oKey.set(infos[0]);
				context.write(oKey, oValue);

			}
			
		}
	}
	
	
	
	
	

	
	//把每个key作为一条记录输出出去, 每个key都不同, 结果都是排过重的
	public static class DesDumplicateReduce extends Reducer<Text, IntWritable, IntWritable, Text>{
		private int sum;
		private IntWritable oValue = new IntWritable();
		
		
		@Override
		protected void reduce(
				//对所有key相同的对进行计算
				Text key, 
				Iterable<IntWritable> values,
				Reducer<Text, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
			
			sum = 0;
			for(IntWritable value : values){
				//System.out.println(key.toString()+"--"+value.toString());
				
				sum += value.get();
			}
			
			oValue.set(sum);
			
			context.write(oValue, key);
			
			
			//System.out.println("统计用户"+key+"的登录次数:**\n"+sum+"次");
		}
		
	}
	

	
	

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DesDumolicateMap.class);
		job.setJobName("计算访问过系统的用户名");
		
		job.setMapperClass(DesDumolicateMap.class);
		job.setReducerClass(DesDumplicateReduce.class);
		
		
		
		
		
		//设置map的输出kv类型和整个mr job的map输出kv类型
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		
		
		
		
		
		
		
		
		
		//设置输入数据
		Path inputPath = new Path("/user/user-logs-large.txt");
		Path outputPath = new Path("/UserVisitTimes");
		
		
		
		
		
		
		
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		
		
		
		
		
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		
		
		
		
		
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		
		
		
		//begin job
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
