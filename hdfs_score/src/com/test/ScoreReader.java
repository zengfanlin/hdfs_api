package com.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class ScoreReader extends RecordReader<Text, Text> {
	private LineReader linereader;
	private Text key;
	private Text value;
	private boolean hasnext;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 保证对象不为空
		key = new Text();
		value = new Text();
		// 强制类型转换
		FileSplit fSplit = (FileSplit) split;
		// 获取split对应的切块
		Path path = fSplit.getPath();
		Configuration conf = new Configuration();
		// 获取文件系统对象
		FileSystem fSystem = path.getFileSystem(conf);
		// 获取文件系统的输入流对象
		FSDataInputStream in = fSystem.open(path);
		linereader = new LineReader(in);
	}

	/*
	 * 判断是否存在下一个kv，有将之赋值 没有就返回false
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		//必须清除一下，否则会重复
		value.clear();
		// 根据实际结构，每次读取4行
		for (int i = 0; i < 4; i++) {
			Text temp = new Text();
			int length = linereader.readLine(temp);
			if (length == 0) {
				return false;
			} else {
				if (i == 0) {
					key.set(temp);
				} else {
					
					value.append(temp.getBytes(), 0, temp.getLength());
					value.append("\r\n".getBytes(), 0, "\r\n".length());
				}
			}
		}
		
		System.out.println("===="+value.toString()+"====");
		
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return hasnext ? 0f : 1f;
	}

	@Override
	public void close() throws IOException {
		linereader.close();
	}

}
