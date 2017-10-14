package com.zhiyou100.bd14.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


public class SecondarySort {
	
	public static class TwoFields implements WritableComparable<TwoFields>{
		private String firstField;
		private String secondField;
		
		
		public String getFirstField() {
			return firstField;
		}
		public void setFirstField(String firstField) {
			this.firstField = firstField;
		}
		public String getSecondField() {
			return secondField;
		}
		public void setSecondField(String secondField) {
			this.secondField = secondField;
		}

		
		//序列化
		@Override
		public void write(DataOutput out) throws IOException {
			
			
		}

		//反序列化
		@Override
		public void readFields(DataInput in) throws IOException {
			
			
		}

		//比较
		@Override
		public int compareTo(TwoFields arg0) {
			
			return 0;
		}
		
	}
}