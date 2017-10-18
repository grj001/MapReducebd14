package com.zhiyou100.bd14.mr.semiJoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SemiJoin2 {
	
	/*
	 * map缓存文件中加载
	 * 在解析数据时把另一张
	 * 表中key和缓存文件中数据
	 * 关联不上的抛出掉
	 * 给两张表kv打标识
	 * 
	 * Reduce根据kv标识把
	 * 数据分成两组
	 * 然后进行笛卡尔乘积
	 */
	
	//用来存放本地缓存文件, 里的信息
	private static HashMap<String, String> userInfos = new HashMap<String, String>();
	
	private List<String> userInfoList;
	
	//ReduceJoinMap
	public static class SemiJoin2Map extends Mapper<LongWritable, Text, Text, Text>{
		private String[] infos;
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("** SemiJoin2Map类中的setop方法中:");
			//获取分布式缓存文件的路径
			URI[] cacheFiles = context.getCacheFiles();
			//获取文件管理系统
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			//遍历uri
			for(URI uri : cacheFiles){
				System.out.println("setop()方法中的地址uri:\t"+uri.toString());
				//只有一个路径user_info.txt
				//如果uri, 包含user_infos.txt, 是这个文件的话
				if(uri.toString().contains("user_info.txt")){
					FSDataInputStream inputStream = fileSystem.open(new Path(uri));
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String line = bufferedReader.readLine();
					while(line != null){
						System.out.println("输出的文件的每一行:\t"+line);
						
						infos = line.split("\\s"); // 0:mike 1:man 2:henan
						//将用户姓名, 用户所在地, 放入userInfos List集合中
						userInfos.put(infos[0], infos[1]+"\t"+infos[2]); // userInfos[0:mike, 2:henan]
						
						line = bufferedReader.readLine(); //再读一行
					}
					
					System.out.println("userInfos:\t"+userInfos);
				}
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//System.out.println("map输入:\t"+key+"\t"+value);
			//如果有相同的用户名就写入Reduce
			//jim	logout	93.24.237.12
			infos = value.toString().split("\\s");
			//所以是索引0
			if(userInfos.containsKey(infos[0])){
				outKey.set(infos[0]);
				outValue.set(infos[1]+"\t"+infos[2]);
				// 输出姓名 = 状态 , ip
				context.write(outKey, outValue);
			}
		}
	}
	
	//ReduceJoinReduce
	public static class SemiJoin2Reduce extends Reducer<Text, Text, Text, Text>{
		private Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			//对两组的数据进行笛卡尔乘积
			Set<String> userInfoSet = userInfos.keySet();
			for(Text value : values){
				for(String userInfo : userInfoSet){
					outValue.set(value.toString()+"\t"+userInfos.get(userInfo));
					context.write(key, outValue);
				}
			}
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "reduce端关联");
		job.setJarByClass(SemiJoin2.class);
		
		job.setMapperClass(SemiJoin2Map.class);
		job.setReducerClass(SemiJoin2Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		//设置分布式缓存文件(小表)
		Path cacheFilePath = new Path("/user/user_info.txt");
		job.addCacheFile(cacheFilePath.toUri());
		
		FileInputFormat.addInputPath(job, new Path("/user/user-logs-large.txt"));
		Path outputDir = new Path("/user/output/SemiJoin2");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}

