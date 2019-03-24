package com.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String data[] = line.split(" ");//13877779999 bj zs 2145
		context.write(new Text(data[0]), new FlowBean(data[0], data[1], data[2], Long.parseLong(data[3])));
	}
}
