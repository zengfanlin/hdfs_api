package com.tedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job_score");
		job.setJarByClass(com.tedu.ScoreDriver.class);
		job.setMapperClass(com.tedu.ScoreMapper.class);

		job.setReducerClass(com.tedu.ScoreReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreBean.class);
		
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		//指定分区类
		
		job.setPartitionerClass(ScorePartitioner.class);
		job.setNumReduceTasks(4);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/park/score"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/park/score/result"));

		if (!job.waitForCompletion(true))
			return;
	}

}
