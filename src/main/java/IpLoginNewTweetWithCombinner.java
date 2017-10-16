import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.zhiyou100.bd14.mr.UserVisitTimes;
import com.zhiyou100.bd14.mr.UserVisitTimes.UserVisitTimesMap;
import com.zhiyou100.bd14.mr.UserVisitTimes.UserVisitTimesReduce;

public class IpLoginNewTweetWithCombinner {

	//Text
	public static class IpLoginNewTweetWithCombinnerMap extends Mapper<Text, Text, Text, Text>{
		private final String ONE_STR = "1";
		private final String ZERO_STR = "0";
		private Text outKey = new Text();
		private Text outValue = new Text();
		private String[] infos;
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if(key.toString() != null &&!key.toString().equals("") && value.toString() != null && !value.toString().equals("")){
				infos = value.toString().split("\\s");//[action_status, ip]
				outKey.set(infos[1]);//outKey ip
				if(infos[0].equals("login") || infos[0].equals("new_tweet")){ 
					outValue.set(	//如果action_status = login or new_tweet
									(infos[0].equals("login") ? ONE_STR : ZERO_STR) 
									+ "\t" + 
								    (infos[0].equals("new_tweet") ? ONE_STR : ZERO_STR)
								);//set action_status
					context.write(outKey, outValue);	//outKey:ip	 outValue:0, 1
					System.out.println(outKey + "\t" +outValue);
				}
			}
		}
	}
	
	public static class IpLoginNewTweetWithCombinnerReudce extends Reducer<Text, Text, Text, Text>{
		private String[] infos;
		private Integer loginTimes;
		private Integer newTweetTimes;
		private Text outValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			loginTimes = 0;	//每次进方法的时候, 使他为0
			newTweetTimes = 0;	//每次进方法的时候, 使他为0
			for(Text value : values){
				infos = value.toString().split("\\s"); //对values进行分解得到 数组 0:0, 1:1
				System.out.println(key + "\t" +Arrays.toString(infos));
				loginTimes += Integer.valueOf(infos[0]);
				newTweetTimes += Integer.valueOf(infos[1]);
			}
			outValue.set(loginTimes + "\t" + newTweetTimes); //outValue = "loginTimes:223 : newTweetTimes:452"
			context.write(key, outValue);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(IpLoginNewTweetWithCombinner.class);
		job.setJobName("IpLoginNewTweetWithCombinner");
		
		job.setMapperClass(IpLoginNewTweetWithCombinnerMap.class);
		job.setCombinerClass(IpLoginNewTweetWithCombinnerReudce.class);
		job.setReducerClass(IpLoginNewTweetWithCombinnerReudce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		Path inputPath = new Path("/user/user-logs-large.txt");
		Path outputPath = new Path("/user/output/IpLoginNewTweetWithCombinner");
		
		outputPath.getFileSystem(conf).delete(outputPath,true);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true)?0:1);	//begin job
	}
}
