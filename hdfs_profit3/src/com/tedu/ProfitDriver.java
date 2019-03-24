package com.tedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfitDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job_profit3");
		job.setJarByClass(com.tedu.ProfitDriver.class);
		job.setMapperClass(com.tedu.ProfitMapper.class);

		job.setReducerClass(com.tedu.ProfitReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(ProfitInfo.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/park/profit/result"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/park/profit/result3"));

		if (!job.waitForCompletion(true))
			return;
	}

}
