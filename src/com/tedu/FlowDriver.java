package com.tedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job_flow2");
		job.setJarByClass(com.tedu.FlowDriver.class);
		job.setMapperClass(com.tedu.FlowMapper.class);

		job.setReducerClass(com.tedu.FlowReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/park/flow/flow.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/park/flow/result"));

		if (!job.waitForCompletion(true))
			return;
	}

}
