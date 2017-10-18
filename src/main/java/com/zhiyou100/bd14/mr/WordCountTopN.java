package com.zhiyou100.bd14.mr;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountTopN {

	public static class WordCountTopNMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final IntWritable ONE = new IntWritable(1);
		private Text outKey = new Text();
		private String[] infos;
		
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String filePath = "";
			FileSplit fileSplit = new FileSplit();
			
			fileSplit = (FileSplit) context.getInputSplit();
			filePath = fileSplit.getPath().toString();
			
			System.out.println("fileSplit:"+fileSplit+"\tfilePath:"+filePath);
		}



		//每进一次方法, 是一行的数据
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//System.out.println("** map 方法-------------------------");
			//System.out.println("map输入一行:\tLongWritable:("+key+")\tText:("+value+")");
			//map输入一行的信息
			//按空格进行分割
			infos = value.toString().split("\\s");
			//循环遍历出每一行单词
			for(String word : infos){
				//排除空和空字符串
				if(word != null && !word.equals("")){
					//设置输出键值为 单词
					outKey.set(word);
					//输出一个单词, 循环后输出,一行的每一个单词 ,对应的值是1, 用于以后的计数
					context.write(outKey, ONE);
					//System.out.println("map输出单词:\tword:("+outKey+")\t字数:("+ONE+")");
				}
			}
		}
	}
	
	
	
	public static class WordCountTopNCombinner extends Reducer<Text, IntWritable, Text, IntWritable>{
		private int sum;
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();
		//开辟空间保存topN
		//tree是一个排序的map, 按照key进行排序
		private TreeMap<Integer, String> topN = new TreeMap<Integer, String>();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//System.out.println("** combinner 方法-------------------------");
			//System.out.println("Combinner进行输入同一个键值:\t"+key);
			sum = 0;
			for(IntWritable value : values){
				//System.out.println("不同的value:\t"+ value);
				sum += value.get();
			}
			//System.out.println("单词出现的次数进行统计得sum:\t"+sum);
			//把计算结果放入topN, 如果topN中不满N个元素的画可直接往里面放, 然后还得
			//先看topN中有没有相同的key, 如果有的话,
			//batopN中相同的key对应的value和单词串在一起,如果没有的画直接放进去
			//如果TreeMap topN的大小 小于3 的话,(自动排序)
			if(topN.size() < 3){
				if(topN.get(sum) != null){
					//对应这个出现次数sum有, 就在相同的key后面拼接 key:sum , value:这个单词
					topN.put(sum, topN.get(sum)+"---"+key.toString());
				}else{
					//对应的这个出现次数sum没有, 但是还没有放满, 就在放进去
					topN.put(sum, key.toString());
				}
			}else{
				//大于等于N的话放进去一个 然后再删除掉一个, 始终保持topN中有N个元素
				//如果TreeMap定义的大小, 已经满了, 
				if(topN.get(sum) != null){
					//有相应的出现次数sum, 继续拼接
					topN.put(sum, topN.get(sum)+"---"+key.toString());
				}else{
					//没有相应的出现次数, 就放进去比较
					topN.put(sum, key.toString());
					//删除最小的一个
					topN.remove(topN.lastKey());
				}
			}
			//放进去后TreeMap会自动排序, 这个时候把最后一个再给删除掉, 保证topN中只有N个kv对
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			if(topN != null && !topN.isEmpty()){
				Set<Integer> keys = topN.keySet();
				for(Integer key : keys){
					outKey.set(topN.get(key));
					outValue.set(key);
					context.write(outKey, outValue);
					//System.out.println("Combinner输出:\t单词:("+outKey+")\t出现次数:\t("+outValue+")");
				}
			}
		}
		
	}
	
	
	
	
	
	public static class WordCountTopNReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private int sum;
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();
		//开辟空间保存topN
		//tree是一个排序的map, 按照key进行排序
		private TreeMap<Integer, String> topN = new TreeMap<Integer, String>();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//System.out.println("** reduce 方法-------------------------");
			//System.out.println("reducer进行输入同一个键值, 拼接后的单词:\t"+key);
			sum = 0;
			for(IntWritable value : values){
				//System.out.println("不同的单词出现次数value:\t"+ value);
				sum += value.get();
			}
			//System.out.println("单词出现的次数进行统计得sum:\t"+sum);
			//把计算结果放入topN, 如果topN中不满N个元素的画可直接往里面放, 然后还得
			//先看topN中有没有相同的key, 如果有的话,
			//batopN中相同的key对应的value和单词串在一起,如果没有的画直接放进去
			//如果TreeMap topN的大小 小于3 的话,(自动排序)
			if(topN.size() < 3){
				if(topN.get(sum) != null){
					//对应这个出现次数sum有, 就在相同的key后面拼接 key:sum , value:这个单词
					topN.put(sum, topN.get(sum)+"---"+key.toString());
				}else{
					//对应的这个出现次数sum没有, 但是还没有放满, 就在放进去
					topN.put(sum, key.toString());
				}
			}else{
				//大于等于N的话放进去一个 然后再删除掉一个, 始终保持topN中有N个元素
				//如果TreeMap定义的大小, 已经满了, 
				if(topN.get(sum) != null){
					//有相应的出现次数sum, 继续拼接
					topN.put(sum, topN.get(sum)+"---"+key.toString());
				}else{
					//没有相应的出现次数, 就放进去比较
					topN.put(sum, key.toString());
					//删除最小的一个
					topN.remove(topN.lastKey());
				}
			}
			//放进去后TreeMap会自动排序, 这个时候把最后一个再给删除掉, 保证topN中只有N个kv对
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			if(topN != null && !topN.isEmpty()){
				Set<Integer> keys = topN.keySet();
				for(Integer key : keys){
					outKey.set(topN.get(key));
					outValue.set(key);
					context.write(outKey, outValue);
					//System.out.println("Reudcer,拼接统计后输出:\t单词串:("+outKey+")\t出现次数:\t("+outValue+")");
				}
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountTopN.class);
		job.setJobName("词频topN");
		
		job.setMapperClass(WordCountTopNMap.class);
		job.setCombinerClass(WordCountTopNCombinner.class);
		job.setReducerClass(WordCountTopNReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inputPath = new Path("/user/reversetext");
		Path outputDir = new Path("/user/output/ReversedIndex");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
