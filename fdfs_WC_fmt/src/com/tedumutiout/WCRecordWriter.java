package com.tedumutiout;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WCRecordWriter extends RecordWriter<Text, LongWritable> {
	private FSDataOutputStream out;
	public WCRecordWriter(FSDataOutputStream out) {
		this.out=out;
	}

	@Override
	public void write(Text key, LongWritable value) throws IOException, InterruptedException {
		//编写自定义的输出方式:hello~8#jerry~1#joy~3#rose~2#tom~2#
		out.write((key.toString()+"~"+value+"#").getBytes());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

}
