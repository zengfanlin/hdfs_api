package com.test;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoreMapper extends Mapper<Text, Text, Text, DoubleWritable> {

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String nameString = key.toString();
		String data[] = value.toString().split("\r\n");
		double sum = 0d;

		for (String string : data) {
			sum += Double.parseDouble(string.split(" ")[1]);
		}
		System.out.println("key:" + nameString + " value:" + value.toString());
		context.write(new Text(nameString), new DoubleWritable(sum));
	}

}
