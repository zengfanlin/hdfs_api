package com.tedu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//指定job配置和名称
		Job job = Job.getInstance(conf, "WCJob");
		//指定job执行的类 
		job.setJarByClass(WCDriver.class);
		//指定mapperlw
		job.setMapperClass(WCMapper.class);
		//reduce指定
		job.setReducerClass(WCReducer.class);
		//指定mapper的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//指定reduce的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//指定任务操作的资源的位置
		FileInputFormat .setInputPaths(job, new Path("hdfs://hadoop01:9000/park/word"));
		//指定任务操作结束后结果保存的类型
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/park/result"));
		job.waitForCompletion(true);
	}
}
