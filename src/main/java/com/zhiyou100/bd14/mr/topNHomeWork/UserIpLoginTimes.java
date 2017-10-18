package com.zhiyou100.bd14.mr.topNHomeWork;

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

import com.zhiyou100.bd14.mr.DesDumplicate.DesDumolicateMap;

public class UserIpLoginTimes {

	public static class UserIpLoginTimesMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private String[] infos;
		
		//输出结果outKey, outValue.
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//每个用户在每个ip上的登录次数
			//用户,ip组合成key
			//if判断出状态为login, 输出value为1
			infos = value.toString().split("\\s");
			
			if(infos[1].equals("login")){
				outKey.set(infos[0]+"\t"+infos[2]);
				context.write(outKey, outValue);
			}
		}
		
	}
	
	public static class UserIpLoginTimesReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		//统计同一个用户和ip, 的出现次数, 登录次数
		private int sum;
		
		//输出结果key, outValue.
		private IntWritable outValue = new IntWritable(1);
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			sum = 0;
			//map中输出kv, 输入到reduce中
			// 用户名 ip , 1
			// 进行计数
			for(IntWritable value : values){
				sum += value.get();
			}
			
			outValue.set(sum);
			
			context.write(key, outValue);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserIpLoginTimes.class);
		job.setJobName("每个用户在每个ip上的登录次数");
		
		job.setMapperClass(UserIpLoginTimesMap.class);
		job.setReducerClass(UserIpLoginTimesReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入数据
		Path inputPath = new Path("/user/user-logs-large.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		//设置输出数据(目录不能相同)
		Path outputPath = new Path("/user/output/UserIpLoginTimes");
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		outputPath.getFileSystem(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//begin job
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
