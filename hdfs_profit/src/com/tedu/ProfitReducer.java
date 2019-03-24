package com.tedu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfitReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		String name = key.toString();
		Iterator<LongWritable> iter = values.iterator();
		Long profit = 0L;
		while (iter.hasNext()) {
			profit += iter.next().get();
		}
		context.write(new Text(name), new LongWritable(profit));
	}

}
