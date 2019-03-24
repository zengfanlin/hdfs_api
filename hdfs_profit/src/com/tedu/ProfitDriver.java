package com.tedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfitDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job_profit1");
		job.setJarByClass(com.tedu.ProfitDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(ProfitMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(ProfitReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/park/profit/profit.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/park/profit/result"));

		if (!job.waitForCompletion(true))
			return;
	}

}
