package com.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, ScoreBean> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		FileSplit fs=(FileSplit)context.getInputSplit();
		//获取被处理文件名称
		String fname=fs.getPath().getName();
		String subject=fname.substring(0,fname.lastIndexOf(".")); 
		String line=ivalue.toString();
		//1 zhang 89
		String data[] =line.split(" ");
		ScoreBean sb=new ScoreBean();
		sb.setMonth(Integer.parseInt(data[0]));
		sb.setName(data[1]);
		sb.setScore(Double.parseDouble(data[2]));
		sb.setSubject(subject);
		context.write(new Text(sb.getName()), sb);//按名字分组
	}

}
