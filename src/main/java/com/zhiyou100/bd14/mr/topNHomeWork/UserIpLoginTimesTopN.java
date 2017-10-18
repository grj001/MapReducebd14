package com.zhiyou100.bd14.mr.topNHomeWork;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

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


public class UserIpLoginTimesTopN {
	
	
	
	public static class UserIpLoginTimesTopNMap extends Mapper<LongWritable, Text, Text, Text>{
		private String[] infos;
		
		//输出结果outKey, outValue.
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//每个用户
			//ip 和 登录次数
			//每个用户需要近一个reduce
			
			
			infos = value.toString().split("\\s");
			
			outKey.set(infos[0]);
			outValue.set(infos[1]+"\t"+infos[2]);
			
			
			context.write(outKey, outValue);
		}
	}
	
	public static class UserIpLoginTimesTopNReduce extends Reducer<Text, Text, Text, IntWritable>{
		//建立treeMap进行排序
		private TreeMap<Integer, String> topN = new TreeMap<Integer, String>();
		
		private String[] infos;
		
		//输出结果key, outValue.
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();
		private Integer time = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			//map中输出kv, 输入到reduce中
			//输入的是		姓名                   ip和登录次数(1)
			//需要对登录次数进行排序
			//循环中进行排序
			
			
			
			for(Text ipAndTime : values){
				
				//对IP,time,进行分割
				//ip:0
				//time:1
				infos = ipAndTime.toString().split("\\s");
				
				time = Integer.valueOf(infos[1]);
				
				
				//对key进行拼接
				Text newKey = new Text(key.toString()+"\t"+infos[0]);
				
				
				System.out.println(newKey.toString());
				
				if(topN.size() < 3){
					if(topN.get(time) != null){
						//对应这个出现次数sum有, 就在相同的key后面拼接 key:sum , value:这个单词
						topN.put(time, topN.get(time)+"---"+newKey.toString());
					}else{
						//对应的这个出现次数sum没有, 但是还没有放满, 就在放进去
						topN.put(time, newKey.toString());
					}
				}else{
					//大于等于N的话放进去一个 然后再删除掉一个, 始终保持topN中有N个元素
					//如果TreeMap定义的大小, 已经满了, 
					if(topN.get(time) != null){
						//有相应的出现次数sum, 继续拼接
						topN.put(time, topN.get(time)+"---"+newKey.toString());
					}else{
						//没有相应的出现次数, 就放进去比较
						topN.put(time, newKey.toString());
						//删除最小的一个
						topN.remove(topN.lastKey());
					}
				}
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			System.out.println(topN);
			
			if(topN != null && !topN.isEmpty()){
				
				Set<Integer> keys = topN.keySet();
				
				for(Integer key : keys){
					
					//topN的键是登录次数, 值是姓名和ip
					outKey.set(topN.get(key));
					outValue.set(key);
					
					context.write(outKey, outValue);
				}
				
			}
		}
		
		
		
		
		
		
		
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserIpLoginTimesTopN.class);
		job.setJobName("每个用户在每个ip上的登录次数");
		
		job.setMapperClass(UserIpLoginTimesTopNMap.class);
		job.setReducerClass(UserIpLoginTimesTopNReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入数据
		Path inputPath = new Path("/user/output/UserIpLoginTimes");
		FileInputFormat.addInputPath(job, inputPath);
		
		//设置输出数据(目录不能相同)
		Path outputPath = new Path("/user/output/UserIpLoginTimesTopN");
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		outputPath.getFileSystem(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//begin job
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
