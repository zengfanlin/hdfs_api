package com.tedu;

import java.io.IOException;
import java.nio.channels.NonWritableChannelException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

public class WCOutPutFormat extends FileOutputFormat<Text, LongWritable> {

	@Override
	public org.apache.hadoop.mapreduce.RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		//获取默认的工作输出路径
		Path path = getDefaultWorkFile(job, "");
		//获取代表文件系统的fs
		FileSystem fs=path.getFileSystem(conf);
		
		FSDataOutputStream out=fs.create(path,false);
		return new WCRecordWriter(out);
	}

}
