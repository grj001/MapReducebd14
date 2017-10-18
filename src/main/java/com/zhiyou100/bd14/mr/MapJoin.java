package com.zhiyou100.bd14.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.bd14.mr.ReduceJoin.ReduceJoinMap;
import com.zhiyou100.bd14.mr.ReduceJoin.ReduceJoinReduce;
import com.zhiyou100.bd14.mr.ReduceJoin.ValueWithFlag;

public class MapJoin {

	//map读取分布式缓存文件(小表的数据)
	//把他加载到一个hashMap中
	//关联字段作为key
	//计算相关字段值作为value
	
	//map中处理大表数据
	//每处理一条数据就取出相关字段
	//看hashMap中是否存在,
	//存在代表能关联
	//不存在代表关联不上
	public static class MapJoinMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private HashMap<String, String> userInfos = new HashMap<String, String>();
		private String[] infos;
		private Text outKeys = new Text();
		private IntWritable ONE = new IntWritable(1);
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("** 进入MapJoinMap类中的setop方法中:");
			//获取分布式缓存文件的路径
			URI[] cacheFiles = context.getCacheFiles();
			//获取文件管理系统
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			//遍历uri
			for(URI uri : cacheFiles){
				System.out.println("** 获取分布式缓存文件的路径为:\t"+uri.toString());
				//如果uri, 包含user_infos.txt, 是这个文件的话
				if(uri.toString().contains("user_info.txt")){
					FSDataInputStream inputStream = fileSystem.open(new Path(uri));
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String line = bufferedReader.readLine();
					while(line != null){
						
						infos = line.split("\\s"); // 0:mike 1:man 2:henan
						userInfos.put(infos[0], infos[2]); // userInfos[0:mike, 2:henan]
						outKeys.set(userInfos.get(infos[0])); //outKeys = henan
						
						//向map端输出用户姓名和用户所在省份, 和1
						context.write(outKeys, ONE);
					
						
						line = bufferedReader.readLine(); //再读一行
						
					}
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			infos = value.toString().split("\\s");
			if(userInfos.containsKey(infos[0])){
				outKeys.set(userInfos.get(infos[0]));
				context.write(outKeys, ONE);
				
			}
			
		}
	}
	
	public static class MapJoinReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private int sum;
		private IntWritable outValue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			sum = 0;
			for(IntWritable value : values){
				
				sum += value.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "map端关联");
		job.setJarByClass(MapJoin.class);
		
		job.setMapperClass(MapJoinMap.class);
		job.setReducerClass(MapJoinReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置分布式缓存文件(小表)
		Path cacheFilePath = new Path("/user/user_info.txt");
		job.addCacheFile(cacheFilePath.toUri());
		
		//大表
		FileInputFormat.addInputPath(job, new Path("/user/user-logs-large.txt"));
		Path outputDir = new Path("/user/output/MapJoin");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
