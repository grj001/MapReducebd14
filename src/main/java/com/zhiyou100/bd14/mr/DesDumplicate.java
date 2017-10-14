package com.zhiyou100.bd14.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//计算出访问该系统的每一个用户的用户名
//去重
public class DesDumplicate {
	//从文件中抽取用户名, 并将其作为key 发送到reducer节点, value与节点无关
	public static class DesDumolicateMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		private String[] infos;
		private NullWritable oValue = NullWritable.get();
		private Text oKey = new Text();
		@Override
		protected void map(
				LongWritable key, 
				Text value, 
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//解析一行记录
			infos = value.toString().split("\\s");
			oKey.set(infos[0]);
			context.write(oKey, oValue);
		}
	}
	
	
	//把每个key作为一条记录输出出去, 每个key都不同, 结果都是排过重的
	public static class DesDumplicateReduce extends Reducer<Text, NullWritable, Text, NullWritable>{
		private final NullWritable oValue = NullWritable.get();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> vlaues,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			context.write(key, oValue);
		}
		
	}
	
	
	//构建和启动Job
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DesDumolicateMap.class);
		job.setJobName("计算访问过系统的用户名");
		
		job.setMapperClass(DesDumolicateMap.class);
		job.setReducerClass(DesDumplicateReduce.class);
		
		//设置map的输出kv类型和整个mr job的map输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//设置输入数据
		Path inputPath = new Path("/user/user-logs-large.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		//设置输出数据(目录不能相同)
		Path outputPath = new Path("/bd14/data");
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		outputPath.getFileSystem(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//begin job
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
