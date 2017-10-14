package com.zhiyou100.bd14.mr;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import com.zhiyou100.bd14.mr.UserVisitTimes.DesDumolicateMap;

public class TotalSort {

	
	
	
	
	
	public static class TotalSortMap extends Mapper<LongWritable, Text, IntWritable, Text>{
		private String[] infos;
		private IntWritable oKey = new IntWritable(1);
		private Text oValue = new Text();
		
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("\\s");
			oKey.set(Integer.valueOf(infos[1]));
			oValue.set(infos[0]);
			context.write(oKey, oValue);
			
			System.out.println(oKey.toString()+"--"+oValue.toString());
			
		}
		
		
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static class TotalSortReduce extends Reducer<IntWritable, Text, Text, IntWritable>{
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> vlaues,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			for(Text value : vlaues){
				context.write(value, key);
			}
		}
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception, InterruptedException{
		Configuration conf = new Configuration();
		
		
		
		
		
		
		/* 
		 * 由InputFormat决定
		 * 传送到map
		 * 或者sampler的数据必须是我们想要抽样的key
		 * 从不同的地方拿
		 * inputFormat直接拿, 自己定义api文档
		 * KeyValueTextInoutFormat, 不再是偏移量了
		 * 普通的文本
		 * 根据固定分割符进行, 分割, 没有分隔符, 就不分割
		 * 加到Configuration
		 * 需要颠倒key 和        value的值
		 * 
		 */
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler(0.2, 5);
		
		
		
		
		
		
		
		//设置分区文件
		FileSystem hdFileSystem = FileSystem.get(conf);
		Path partitionFile = new Path("/TotalSort-Partion/_partition");
		
		
		
		
		//设置后, 全排序的partitioner程序就会读取这个分区文件来完成按顺序进行分区
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		
		
		
		
		//Job
		Job job = Job.getInstance(conf);
		job.setJarByClass(DesDumolicateMap.class);
		job.setJobName("全排序");
		
		
		
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(TotalSortReduce.class);
		
		
		
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		
		
		//把分区文件加入分布式缓存中
		job.addCacheFile(partitionFile.toUri());
		
		
		//设置分区器
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		
		//设置节点个数
		job.setNumReduceTasks(2);
		
		
		
		
		
		
		//设置数据, 输入和删除目录
		Path inputPath = new Path("/UserVisitTimes");
		Path outputDir = new Path("/TotalSort");
		
		
		
		
		
		
		
		
		hdFileSystem.delete(outputDir,true);
		
		
		
		
		
		
		//map的输入会把文本文件读取成kv对,  按照分隔符把一行分成两个部分, 前面key
		//后面value, 如果分隔符不存在, 则整行都是key  ,   value为空,	默认分隔符是\t
		//手动指定分隔符参数: mapreduce.input.keyvaluelinerecordreader.key.value.separator
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		//设置输出数据(目录不能相同)
		//得到hdfs文件管理系统, 进行递归删除, 先进行删除
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		
		
		
		
		
		
		
		
		//将随机抽样放入分区文件
		InputSampler.writePartitionFile(job, sampler);
		
		
		
		
		
		//begin job
		System.exit(job.waitForCompletion(true)?0:1);
		
		
		
		
		
		
		
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
