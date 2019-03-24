package com.tedu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * <Text, ScoreBean>和map输出参数一致
 * 
 * @author Administrator
 *
 */
public class ScorePartitioner extends Partitioner<Text, ScoreBean> {

	@Override
	public int getPartition(Text key, ScoreBean value, int num) {
		// 返回分区值，按月份分区
		return value.getMonth() - 1;//一月分在0分区，二月分在1分区
	}

}
