package com.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, ProfitInfo, NullWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();

		String data[] = line.split("\t");
		ProfitInfo pi = new ProfitInfo();
		pi.setName(data[0]);
		pi.setProfit(Long.parseLong(data[1]));
		context.write(pi, NullWritable.get());//把实现比较接口的类重新输出即可

	}

}
