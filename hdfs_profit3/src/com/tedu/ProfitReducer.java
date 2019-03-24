package com.tedu;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfitReducer extends Reducer<ProfitInfo, NullWritable, ProfitInfo, NullWritable> {

	@Override
	protected void reduce(ProfitInfo key, Iterable<NullWritable> values,
			Reducer<ProfitInfo, NullWritable, ProfitInfo, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}

}
