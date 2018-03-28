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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.bd14.mr.DesDumplicate.DesDumolicateMap;

public class MRChain {
	private static Path path;

	//MapReudce中经过的第一个Map, 过滤掉销售数量大于一亿的数据
	public static class MRChainMap1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();
		private String[] infos;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = new FileSplit();
			path = fileSplit.getPath();
			System.out.println("** 进入map方法:=================,读取文件:"+path);
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//读取文件的一行的数据:	商品名称	价格
			System.out.println("读取文件"+path+"的一行数�?"+value);
			infos = value.toString().split("\\s");
			if(Integer.valueOf(infos[1]) <= 100000000){
				outKey.set(infos[0]);
				outValue.set(Integer.valueOf(infos[1]));
				context.write(outKey, outValue);
			}
		}
	}


	//过滤掉数量在100-100000之间的数�?
	public static class MRChainMap2 extends Mapper<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			if(value.get() <= 100 || value.get() >= 10000){
				context.write(key, value);
			}
		}
	}

	
	//聚合商品总数�?
	public static class MRChainReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private int sum;
		private IntWritable outValue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			sum = 0;
			for(IntWritable value : values){
				sum +=value.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
		}
	}
	
	//商品名称大于三的过滤�?
	public static class MRChainMap3 extends Mapper<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			if(key.getLength() < 3){
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DesDumolicateMap.class);
		job.setJobName("chain mapper");
		
		//设置map端执�?
		ChainMapper.addMapper(job, MRChainMap1.class, LongWritable.class, 
				Text.class, Text.class, IntWritable.class, conf);
		ChainMapper.addMapper(job, MRChainMap1.class, 
				Text.class, IntWritable.class, 
				Text.class, IntWritable.class, conf);
		
		//设置reduce端执�?
		ChainReducer.setReducer(job, MRChainReduce.class, 
				Text.class, IntWritable.class, 
				Text.class, IntWritable.class, conf);
		ChainMapper.addMapper(job, MRChainMap1.class, 
				Text.class, IntWritable.class, 
				Text.class, IntWritable.class, conf);
		
		//设置输入数据
		Path inputPath = new Path("/user/user-logs-large.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		//设置输出数据(目录不能相同)
		Path outputPath = new Path("/bd14/data");
		//得到hdfs文件管理系统, 进行递归删除, 先进行删�?
		outputPath.getFileSystem(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//begin job
		System.exit(job.waitForCompletion(true)?0:1);
	}






















}
