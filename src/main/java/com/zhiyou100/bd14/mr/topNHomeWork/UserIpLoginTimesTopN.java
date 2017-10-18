package com.zhiyou100.bd14.mr.topNHomeWork;

import java.io.File;
import java.io.FileWriter;
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

import com.zhiyou100.bd14.mr.SecondarySort.GroupToReducerComparetor;
import com.zhiyou100.bd14.mr.SecondarySort.TwoFields;

public class UserIpLoginTimesTopN {

	public static class UserIpLoginTimesTopNMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		private String[] infos;

		// 输出结果outKey, outValue.
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();

		@Override
		protected void map(

				LongWritable key, Text value,

				Mapper<

						LongWritable, Text, Text, IntWritable

				>.Context context) throws IOException, InterruptedException {
			// 每个用户
			// ip 和 登录次数
			// 每个用户需要近一个reduce

			infos = value.toString().split("\\s");

			outKey.set(infos[0] + "\t" + infos[1]);
			outValue.set(Integer.valueOf(infos[2]));

			context.write(outKey, outValue);

		}
	}

	public static class UserNamePartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {

			String userName = key.toString().split("\\s")[0];
			
			
			
			
			
			
			
			try {
				
				FileWriter fileWriter = 
						new FileWriter(
								new File("/partition.txt")
								);
				
				fileWriter.write("按照userName分区" + userName+"\n");
				fileWriter.flush();
				fileWriter.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}

			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			System.out.println("按照userName分区" + userName);

			return (userName.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}

	public static class UserIpLoginTimesTopNReduce extends Reducer<

			Text, IntWritable, 
			Text, IntWritable> {

		// 建立treeMap进行排序
		private TreeMap<Integer, String> topN;

		private String[] infos;

		// 输出结果key, outValue.
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();

		@Override
		protected void reduce(

				Text key, Iterable<IntWritable> values,

				Reducer<

						Text, IntWritable, 
						Text, IntWritable>.Context context)
		
				throws IOException, InterruptedException {

			// reduce类只有一个? , 但是reduce()方法运行的此次数对应分区个数
			topN = new TreeMap<Integer, String>();

			for (IntWritable time : values) {

				System.out.println(key + "\t" + time);

				Integer loginTimeInt = time.get();

				if (topN.size() < 3) {
					if (topN.get(loginTimeInt) != null) {
						// 对应这个出现次数sum有, 就在相同的key后面拼接 key:sum , value:这个单词
						topN.put(loginTimeInt, topN.get(loginTimeInt) + "---" + key.toString().split("\\s")[1]);
					} else {
						// 对应的这个出现次数sum没有, 但是还没有放满, 就在放进去
						topN.put(loginTimeInt, key.toString());
					}
				} else {
					// 大于等于N的话放进去一个 然后再删除掉一个, 始终保持topN中有N个元素
					// 如果TreeMap定义的大小, 已经满了,
					if (topN.get(loginTimeInt) != null) {
						// 有相应的出现次数sum, 继续拼接
						topN.put(loginTimeInt, topN.get(loginTimeInt) + "---" + key.toString().split("\\s")[1]);
					} else {
						// 没有相应的出现次数, 就放进去比较
						topN.put(loginTimeInt, key.toString());
						// 删除最小的一个
						topN.remove(topN.lastKey());
					}
				}
			}

			
			
			
			if (topN != null && !topN.isEmpty()) {

				Set<Integer> keys = topN.keySet();

				for (Integer k : keys) {

					outKey.set(topN.get(k));
					outValue.set(k);

					context.write(outKey, outValue);
				}

			}

		}

		@Override
		protected void cleanup(

				Reducer<

						Text, IntWritable, Text, IntWritable>.Context context)

				throws IOException, InterruptedException {

		}

	}

	
	
	//groupingComparetor
	public static class GroupToReducerComparetor extends WritableComparator{
		//构造方法里面要向父类传递
		public GroupToReducerComparetor() {
			//是否实例化对象
			super(Text.class,true);
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
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserIpLoginTimesTopN.class);
		job.setJobName("每个用户在每个ip上的登录次数topN");

		job.setMapperClass(UserIpLoginTimesTopNMap.class);
		job.setPartitionerClass(UserNamePartitioner.class);
		job.setReducerClass(UserIpLoginTimesTopNReduce.class);
		//设置分区后, 需要设置分组后才能, 分组操作
		job.setGroupingComparatorClass(GroupToReducerComparetor.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置输入数据
		Path inputPath = new Path("/user/output/UserIpLoginTimes");
		FileInputFormat.addInputPath(job, inputPath);

		// 设置输出数据(目录不能相同)
		Path outputPath = new Path("/user/output/UserIpLoginTimesTopN");
		// 得到hdfs文件管理系统, 进行递归删除, 先进行删除
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);

		// begin job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
