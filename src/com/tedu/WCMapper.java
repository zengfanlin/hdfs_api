package com.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * hello tom hello joy hello rose hello joy hello jerry hello tom hello rose
 * hello joy
 * 
 * 
 * 
 * @author Administrator 四个形参： 1.输入的key类型偏移量 2.輸入的value類型，读取到的一行文本 3.輸出key類型
 *         hello或tom一個单词 4.輸出value類型 单词的个数
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 * 第一第二个为输入参数context，往外输出的对象第三个第四个为输出的参数
	 * 
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		System.out.println("map_in:" + key.get() + " " + line);
		String[] words = line.split("\\t");
		for (String word : words) {
			System.out.println(word + " ,1");
			context.write(new Text(word), new LongWritable(1));// 表示处理map、的时候，有一个单词就记一次1，输出到reducer

		}
//		map_in:0 hello	tom
//		hello ,1
//		tom ,1
//		map_in:14 hello	joy
//		hello ,1
//		joy ,1
//		map_in:25 hello	rose
//		hello ,1
//		rose ,1
//		map_in:37 hello	joy
//		hello ,1
//		joy ,1
//		map_in:48 hello	jerry
//		hello ,1
//		jerry ,1
//		map_in:61 hello	tom
//		hello ,1
//		tom ,1
//		map_in:72 hello	rose
//		hello ,1
//		rose ,1
//		map_in:84 hello	joy
//		hello ,1
//		joy ,1
//		super.map(key, value, context);
	}
}
