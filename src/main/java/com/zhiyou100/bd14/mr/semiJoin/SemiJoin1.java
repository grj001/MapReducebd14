package com.zhiyou100.bd14.mr.semiJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SemiJoin1 {

	//ValueWithFlag
	public static class ValueWithFlag implements Writable{
		private String value;
		private String flag;
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		public String getFlag() {
			return flag;
		}
		public void setFlag(String flag) {
			this.flag = flag;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(value);
			out.writeUTF(flag);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.value = in.readUTF();
			this.flag = in.readUTF();
		}
		@Override
		public String toString() {
			return value + "\t" + flag;
		}
	}
	
	
	// ReduceJoinMap
	public static class SemiJoin1Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		private String[] infos;
		
		private Text outKey = new Text();
		private NullWritable outValue = NullWritable.get();

		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("** 小表文件的map输入:\t" + key + "\t" + value);
			//向对象中设置文件名称, 
			//对象中有 文件名属性和性别省份属性
			// 需要设置输出的key为 用户的姓名
			infos = value.toString().split("\\s");
			outKey.set(infos[0]); // 0:mike 1:man 2:henan
			
			context.write(outKey, outValue);
			System.out.println("** 小表文件的map输出:\t" + outKey + "\t" + outValue);
		}
	}

	// ReduceJoinReduce
	public static class SemiJoin1Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "reduce端关联");
		job.setJarByClass(SemiJoin1.class);

		job.setMapperClass(SemiJoin1Map.class);
		job.setReducerClass(SemiJoin1Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path("/user/user_info.txt"));
		Path outputDir = new Path("/user/output/SemiJoin1");
		outputDir.getFileSystem(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(job, outputDir);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
