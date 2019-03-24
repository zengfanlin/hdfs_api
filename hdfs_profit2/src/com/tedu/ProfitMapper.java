package com.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		//获取每一行的内容
		String line=value.toString();
		String data[]=line.split("\t");
		//类型转换
		long profit=Long.parseLong(data[1]);
		context.write(new LongWritable(profit), new Text(line));
	}
}
