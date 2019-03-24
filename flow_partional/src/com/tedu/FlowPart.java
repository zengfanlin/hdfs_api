package com.tedu;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPart extends Partitioner<Text, FlowBean> {
	static Map<String, Integer> map;
	static {
		map = new HashMap<>();
		map.put("bj", 0);
		map.put("sh", 1);
		map.put("sz", 2);
	}

	@Override
	public int getPartition(Text key, FlowBean value, int numpart) {
		String addr = value.getAddr();// 根据addr地址分区
		Integer num = map.get(addr);

		if (num == null) {
			return 3;

		} else {
			return num;
		}

	}
}
