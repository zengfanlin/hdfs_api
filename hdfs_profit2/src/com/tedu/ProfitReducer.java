package com.tedu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfitReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		Iterator<Text> iter=values.iterator();
		while (iter.hasNext()) {
			context.write(iter.next(), NullWritable.get());
		}
	}
}
