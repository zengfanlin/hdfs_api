package com.test;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * 继承FileInputFormat 每4行解析一次
 * // 不切块，因为每4行读取一次，切块会造成读取不完整
 * @author Administrator
 *
 */
public class ScoreInputFormat extends FileInputFormat<Text, Text> {
	
	@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			// 不切块，因为每4行读取一次，切块会造成读取不完整
			return false;
		}
	
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new ScoreReader();
	}

	
	
}
