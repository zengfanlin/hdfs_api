package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job_4score");
		job.setJarByClass(com.test.ScoreDriver.class);
		job.setMapperClass(com.test.ScoreMapper.class);

		// TODO: specify a reducer
		job.setReducerClass(ScoreReducer.class);

		// TODO: specify output types
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(ScoreInputFormat.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/park/score1"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/park/score1/result"));

		if (!job.waitForCompletion(true))
			return;
	}

}
