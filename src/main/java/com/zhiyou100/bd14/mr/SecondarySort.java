package com.zhiyou100.bd14.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


public class SecondarySort {
	
	public static class TwoFields implements WritableComparable<TwoFields>{
		private String firstField;
		private Integer secondField;
		
		
		public String getFirstField() {
			return firstField;
		}
		public void setFirstField(String firstField) {
			this.firstField = firstField;
		}
		public Integer getSecondField() {
			return secondField;
		}
		public void setSecondField(Integer secondField) {
			this.secondField = secondField;
		}

		
		//序列化, 保持一致
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(firstField);
			out.writeInt(secondField);
		}

		//反序列化, 保持一致
		@Override
		public void readFields(DataInput in) throws IOException {
			this.firstField = in.readUTF();
			this.secondField = in.readInt();
		}

		/*
		 * 比较
		 * 第一个字段
		 * 第二个字段
		 */
		@Override
		public int compareTo(TwoFields o) {
			if(this.firstField.equals(o.firstField)){
				/*if(this.secondField > o.secondField){
					return 1;
				}else if(this.secondField < o.secondField){
					return -1;
				}else{
					return 0;
				}*/
				return this.secondField - o.secondField;
			}else {
				return this.firstField.compareTo(o.firstField);
			}
		}
	}
	
	
	
	//自定义分区
	public static class TwoFieldsPartitioner extends Partitioner<TwoFields, NullWritable>{
		//return 数字
		@Override
		public int getPartition(TwoFields key, NullWritable value, int numPartitions) {
			int reduceNo = (key.firstField.hashCode() & Integer.MAX_VALUE) % numPartitions;
			return reduceNo;
		}
	}
	
	
	
	//map
	public static class SecondarySortMap extends Mapper<Text, Text, TwoFields, NullWritable>{
		private final NullWritable oValue = NullWritable.get();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, TwoFields, NullWritable>.Context context)
				throws IOException, InterruptedException {
			TwoFields twoFields = new TwoFields();
			twoFields.setFirstField(key.toString());
			twoFields.setSecondField(Integer.valueOf(value.toString()));
			context.write(twoFields, oValue);
		}
	}
	
	
	
	//reduce
	public static class SecondarySortReduce extends Reducer<TwoFields, NullWritable, Text, Text>{
		private Text oKey = new Text();
		private Text oValue = new Text();
		@Override
		protected void reduce(TwoFields key, Iterable<NullWritable> values,
				Reducer<TwoFields, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			for(NullWritable value : values){
				oKey.set(key.firstField);
				oValue.set(String.valueOf(key.secondField));
				context.write(oKey, oValue);
			}
			oKey.set("-----------");
			oValue.set("-----------");
			context.write(oKey, oValue);
		}
	}
	
	
	
	//groupingComparetor
	public static class GroupToReducerComparetor extends WritableComparator{
		//构造方法里面要向父类传递
		public GroupToReducerComparetor() {
			//是否实例化对象
			super(TwoFields.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TwoFields ca = (TwoFields) a;
			TwoFields cb = (TwoFields) b;
			
			return ca.getFirstField().compareTo(cb.getFirstField());
		}
	}
	
	
	
	//main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SecondarySort.class);
		job.setJobName("二次排序");
		
		job.setMapperClass(SecondarySortMap.class);
		job.setReducerClass(SecondarySortReduce.class);
		
		job.setMapOutputKeyClass(TwoFields.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path inputPath = new Path("/user/secondaryorder");
		Path outputDir = new Path("/output/SecondarySort");
		
		outputDir.getFileSystem(conf).delete(outputDir,true);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//读取kv, 发送到map
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setPartitionerClass(TwoFieldsPartitioner.class);
		
		//分组比较器
		job.setGroupingComparatorClass(GroupToReducerComparetor.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
