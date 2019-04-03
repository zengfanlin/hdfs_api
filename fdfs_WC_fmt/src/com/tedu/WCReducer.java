package com.tedu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.omg.CORBA.DynAnyPackage.Invalid;

/**
 * @author Administrator 第一个参数：输入key的类型 第二个参数：输入value集合中元素的类型 3：输出key类型 4:
 *         输出value类型
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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

//		while (vals.hasNext()) {
//			count = count + vals.next().get();
//		}
		System.out.println("reduce_in:" + key.toString() + " " + in_val);
		context.write(key, new LongWritable(count));
		System.out.println("reduce_out:" + key.toString() + count);
//		reduce_in:hello org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@5eea88c9
//		reduce_out:hello8
//		reduce_in:jerry org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@5eea88c9
//		reduce_out:jerry1
//		reduce_in:joy org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@5eea88c9
//		reduce_out:joy3
//		reduce_in:rose org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@5eea88c9
//		reduce_out:rose2
//		reduce_in:tom org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@5eea88c9
//		reduce_out:tom2
	}
}
