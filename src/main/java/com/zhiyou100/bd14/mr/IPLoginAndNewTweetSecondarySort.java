package com.zhiyou100.bd14.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.bd14.mr.SecondarySort.TwoFieldsPartitioner;


public class IPLoginAndNewTweetSecondarySort {

	public static class LoginAndTweetTimes implements WritableComparable<LoginAndTweetTimes>{
		private String ip;
		private Integer loginTimes;
		private Integer tweetTimes;
		
		public String getIp() {
			return ip;
		}
		public void setIp(String ip) {
			this.ip = ip;
		}
		public Integer getLoginTimes() {
			return loginTimes;
		}
		public void setLoginTimes(Integer loginTimes) {
			this.loginTimes = loginTimes;
		}
		public Integer getTweetTimes() {
			return tweetTimes;
		}
		public void setTweetTimes(Integer tweetTimes) {
			this.tweetTimes = tweetTimes;
		}
		
	
		@Override
		public String toString() {
			return ip+"\t"+loginTimes + "\t" + tweetTimes;
		}
		
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(ip);
			out.writeInt(loginTimes);
			out.writeInt(tweetTimes);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.ip = in.readUTF();
			this.loginTimes = in.readInt();
			this.tweetTimes = in.readInt();
			
		}
		@Override
		public int compareTo(LoginAndTweetTimes o) {
			if(this.loginTimes == o.loginTimes){
				return this.tweetTimes - this.tweetTimes;
			}else{
				return this.loginTimes.compareTo(o.loginTimes);
			}
		}
	}
	
	
	
	//自定义分区
	public static class LoginAndTweetTimesPartitioner extends Partitioner<LoginAndTweetTimes, NullWritable>{
		@Override
		public int getPartition(LoginAndTweetTimes key, NullWritable value, int numPartitions) {
			int reduceNo = (key.loginTimes.hashCode() & Integer.MAX_VALUE) % numPartitions;
			return reduceNo;
		}
	}
	
		
	//map
	public static class IPLoginAndNewTweetSecondarySortMap extends Mapper<LongWritable, Text, LoginAndTweetTimes, NullWritable>{
		private NullWritable oValue = NullWritable.get();
		private LoginAndTweetTimes oKey = new LoginAndTweetTimes();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LoginAndTweetTimes, NullWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println(key+"--"+value);
			oKey = new LoginAndTweetTimes();
			
			String[] infos = value.toString().split("\\s");
			oKey.setIp(infos[0]);
			oKey.setLoginTimes(Integer.valueOf(infos[1]));
			oKey.setTweetTimes(Integer.valueOf(infos[2]));
			
			context.write(oKey, oValue);
			System.out.println(oKey+"--"+oValue);
		}
	}
	
	
	//reduce
	public static class IPLoginAndNewTweetSecondarySortReduce extends Reducer<LoginAndTweetTimes, NullWritable, Text, Text>{
		private Text oKey = new Text();
		private Text oValue = new Text();
		@Override
		protected void reduce(LoginAndTweetTimes key, Iterable<NullWritable> values,
				Reducer<LoginAndTweetTimes, NullWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(NullWritable value : values){
				oKey.set(key.ip);
				oValue.set(key.loginTimes.toString()+"\t"+key.tweetTimes.toString());
				context.write(oKey, oValue);
			}
		}
		
		
	}
	
	
	
	//main
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(IPLoginAndNewTweetSecondarySort.class);
		job.setJobName("对ip进行排序");
		
		job.setMapperClass(IPLoginAndNewTweetSecondarySortMap.class);
		job.setReducerClass(IPLoginAndNewTweetSecondarySortReduce.class);
		
		job.setMapOutputKeyClass(LoginAndTweetTimes.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path inputPath = new Path("/IPLoginAndNewTweet");
		Path outputDir = new Path("/IPLoginAndNewTweetSecondarySort");
		
		outputDir.getFileSystem(conf).delete(outputDir,true);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
