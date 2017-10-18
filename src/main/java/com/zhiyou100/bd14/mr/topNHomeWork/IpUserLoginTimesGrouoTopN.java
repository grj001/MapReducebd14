package com.zhiyou100.bd14.mr.topNHomeWork;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IpUserLoginTimesGrouoTopN {

	private static class IpUserLoginTimesGrouoTopNMap extends Mapper<LongWritable, Text, Text, Text> {

		private String[] infos;
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("\\s");
			outKey.set(infos[0]);
			outValue.set(infos[1] + "	" + infos[2]);
			context.write(outKey, outValue);
		}

	}

	
	
/*	public static class IpUserLoginTimesGrouoTopNParTitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {

			String userName = key.toString().split("\\s")[0];

			System.out.println("按照userName分区" + userName);

			return (userName.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}*/

	
	
	
	private static class IpUserLoginTimesGrouoTopNReduce 
	extends Reducer<
	
	Text, Text, 
	Text, IntWritable> {
		
		private TreeMap<Integer, String> topN;

		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();

		private String[] infos;

		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			
			
			topN = new TreeMap<Integer, String>();
			
			
			
			System.out.println(key);
			
			
			
			for (Text value : values) {
				System.out.println("对应尅有几个value" + key + "\t" + value);
				infos = value.toString().split("\\s");

				if (topN.size() < 3) {
					// 注意将这topN里面的Key和value对应的是map（这个map不是mapredicue里面的map）里面的
					if (topN.get(Integer.parseInt(infos[1])) != null) {
						topN.put(
								Integer.parseInt(infos[1]),
								topN.get(Integer.parseInt(infos[1])) + "---" + key.toString() + "\t" + infos[0]);
					} else {
						topN.put(Integer.parseInt(infos[1]), key.toString() + "\t" + infos[0]);
					}
				} else {

					if (topN.get(Integer.parseInt(infos[1])) != null) { // sum表示map输出的的k值

						topN.put(
								Integer.parseInt(infos[1]),
								topN.get(Integer.parseInt(infos[1])) + "---" + key.toString() + "\t" + infos[0]);// 表示将map【对应的键所对应的内容】进行追加这个追加的内容为【reduce对应的键的值】
					} else {
						
						topN.put(Integer.parseInt(infos[1]), key.toString() + "\t" + infos[0]);
						topN.remove(topN.lastKey());
					}
				}
				
			}
			
			
			System.out.println("\t" + "开始分组");
			if (topN != null && !topN.isEmpty()) {

				Set<Integer> keys = topN.keySet();
				
				for (Integer keyss : keys) {
					
					
					outKey.set(topN.get(keyss));
					outValue.set(keyss);

					context.write(outKey, outValue);

				}
			}
			
			
			
			
			
			
		}
	}

	// groupingComparetor
	public static class ReduceGropCompartor extends WritableComparator {
		// 构造方法里面要向父类传递
		public ReduceGropCompartor() {
			// 是否实例化对象
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text ca = (Text) a;
			Text cb = (Text) b;

			String userNameA = ca.toString().split("\\s")[0];
			String userNameB = cb.toString().split("\\s")[0];

			return userNameA.compareTo(userNameB);
		}
	}

	// 组装一个job到mr引擎上执行
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();


		Job job = Job.getInstance(configuration);

		job.setJarByClass(IpUserLoginTimesGrouoTopN.class);
		job.setJobName("分组排序");
		
		
		
		// 配置mr执行类
		job.setMapperClass(IpUserLoginTimesGrouoTopNMap.class);
		//job.setPartitionerClass(IpUserLoginTimesGrouoTopNParTitioner.class);
		job.setReducerClass(IpUserLoginTimesGrouoTopNReduce.class);
		job.setGroupingComparatorClass(ReduceGropCompartor.class);
		
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 设置reduce输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		
		// 设置数据源
		Path inputPath = new Path("/user/output/UserIpLoginTimes");
		// 设置目标文件存放位置
		Path outputPath = new Path("/user/output/IpUserLoginTimesGrouoTopN");

		
		
		
		outputPath.getFileSystem(configuration).delete(outputPath, true);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
