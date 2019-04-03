package com.tedumutiout;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.omg.CORBA.DynAnyPackage.Invalid;

/**
 * @author Administrator 第一个参数：输入key的类型 第二个参数：输入value集合中元素的类型 3：输出key类型 4:
 *         输出value类型
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	private MultipleOutputs<Text, LongWritable> out;

	@Override
	protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// 实例化
		out=new MultipleOutputs<>(context);
		super.setup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		Iterator<LongWritable> vals = values.iterator();
		long count = 0;

		String in_val = "";
		while (vals.hasNext()) {
			long val = vals.next().get();
			count += val;
			in_val = in_val + "," + val;
		}
		//为输出指定的key设置对应的标记
		if(key.toString().matches("^[a-j][a-z]*$")) {
			out.write("small", key,new LongWritable(count));
		}
		else {
			out.write("big", key,new LongWritable(count));
		}
//		context.write(key, new LongWritable(count));
		
	}
}
