package com.tedu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values,
			Reducer<Text, FlowBean, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		String phone = key.toString();
		String name = "";
		Long flow = 0L;
		Iterator<FlowBean> iter = values.iterator();
		while (iter.hasNext()) {
			FlowBean fb = iter.next();
			name = fb.getName();
			flow = flow + fb.getFlow();

		}
		context.write(new Text(phone + " " + name + " " + flow), NullWritable.get());//按 手机号、名字、流量分组
	}
}
