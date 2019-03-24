package com.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String data[] = line.split(" ");
		String name = data[1];
		Long profit = Long.parseLong(data[2]) - Long.parseLong(data[3]);
		context.write(new Text(name), new LongWritable(profit));
	}

}
