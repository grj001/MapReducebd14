package com.zhiyou100.bd14.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IpLoginNewTweet {

	public static class IpLoginNewTweetReduce extends Reducer<Text, Text, Text, Text>{
		private int loginTimes;
		private int newTweetTimes;
		private Text oValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			loginTimes = 0;
			newTweetTimes = 0;
			for(Text value : values){
				if(value.toString().equals("login")){
					loginTimes += 1;
				}else if(value.toString().equals("new_tweet")){
					newTweetTimes += 1;
				}
			}
			oValue.set(loginTimes+"\t"+newTweetTimes);
			context.write(key, oValue);
		}
		
		
	}
}
