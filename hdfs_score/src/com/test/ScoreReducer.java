package com.test;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		double dd=0d;
		for (DoubleWritable doubleWritable : values) {
			dd+=Double.parseDouble(doubleWritable.toString());
		}
		context.write(key, new DoubleWritable(dd));
	}
}
