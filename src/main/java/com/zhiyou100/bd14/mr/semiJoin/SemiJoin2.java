package com.zhiyou100.bd14.mr.semiJoin;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.bd14.mr.ReduceJoin;

public class SemiJoin2 {

	// ValueWithFlag
	public static class ValueWithFlag implements Writable {
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
			return "ValueWithFlag [value=" + value + ", flag=" + flag + "]";
		}
	}

	// ReduceJoinMap
	public static class SemiJoin2Map extends Mapper<LongWritable, Text, Text, ValueWithFlag> {
		private FileSplit inputSplit;
		private String fileName;
		private String[] infos;
		private Text outKey = new Text();
		private ValueWithFlag outValue = new ValueWithFlag();
		
		
		private HashMap<String, String> userInfos = new HashMap<String, String>();
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, ValueWithFlag>.Context context)
				throws IOException, InterruptedException {
			
			
			
			
			URI[] cacheFiles = context.getCacheFiles();
			//获取文件管理系统
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			//遍历uri
			
			
			for(URI uri : cacheFiles){
				
				//如果uri, 包含user_infos.txt, 是这个文件的话
				if(uri.toString().contains("part-r-00000")){
					FSDataInputStream inputStream = fileSystem.open(new Path(uri));
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					
					//line已经是一行记录, 用户名
					//将用户名放入userInfos
					String line = bufferedReader.readLine();
					
					
					while(line != null){
						
						userInfos.put(line, ""); 
						
						
						line = bufferedReader.readLine(); //再读一行
						
					}
				}
				
				
			}
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			inputSplit = (FileSplit) context.getInputSplit();
			if (inputSplit.getPath().toString().contains("user-logs-large.txt")) {
				fileName = "userLogsLarge";
			} else if (inputSplit.getPath().toString().contains("user_info.txt")) {
				fileName = "userinfo";
			}
			System.out.println("获取到文件名称:\t" + fileName);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, ValueWithFlag>.Context context)
				throws IOException, InterruptedException {
			
			outValue.setFlag(fileName);
			
			infos = value.toString().split("\\s");
			
			if(userInfos.containsKey(infos[0])){
				if (fileName.equals("userLogsLarge")) {
					outKey.set(infos[0]);
					outValue.setValue(infos[1] + "\t" + infos[2]);
				} else if (fileName.equals("userinfo")) {
					outKey.set(infos[0]); // 0:mike 1:man 2:henan
					outValue.setValue(infos[1] + "\t" + infos[2]); // 0:mike 1:man
																	// 2:henan
				}
			}
			
			
			context.write(outKey, outValue);
			
		}
	}

	// ReduceJoinReduce
	public static class SemiJoin2Reduce extends Reducer<Text, ValueWithFlag, Text, Text> {
		private List<String> userLogsLargeList;
		private List<String> userInfoList;
		private Text outValue = new Text();

		@Override
		protected void reduce(Text key, Iterable<ValueWithFlag> values,
				Reducer<Text, ValueWithFlag, Text, Text>.Context context) throws IOException, InterruptedException {
			System.out.println("** 进入Reduce reduce()方法:\t输入为key为:\t" + key);
			userLogsLargeList = new ArrayList<String>();
			userInfoList = new ArrayList<String>();
			for (ValueWithFlag value : values) {
				System.out.println("** 进入Reduce reduce()方法中的for循环:\t输入为value为:\t" + value);
				if (value.getFlag().equals("userLogsLarge")) {
					userLogsLargeList.add(value.getValue());
				} else if (value.getFlag().equals("userinfo")) {
					userInfoList.add(value.getValue());
				}
			}
			// 对两组的数据进行笛卡尔乘积
			for (String userLogsLarge : userLogsLargeList) {
				for (String userInfo : userInfoList) {
					outValue.set(userLogsLarge + "\t" + userInfo);
					context.write(key, outValue);
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "reduce端关联");
		job.setJarByClass(ReduceJoin.class);

		job.setMapperClass(SemiJoin2Map.class);
		job.setReducerClass(SemiJoin2Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ValueWithFlag.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		//设置分布式缓存文件(小表)
		Path cacheFilePath = new Path("/user/output/SemiJoin1/part-r-00000");
		job.addCacheFile(cacheFilePath.toUri());
		
		
		FileInputFormat.addInputPath(job, new Path("/user/user_info.txt"));
		FileInputFormat.addInputPath(job, new Path("/user/user-logs-large.txt"));
		Path outputDir = new Path("/user/output/ReduceJoin");
		outputDir.getFileSystem(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(job, outputDir);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
