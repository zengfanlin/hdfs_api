package com.tedu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, ScoreBean, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<ScoreBean> values,
			Reducer<Text, ScoreBean, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		Iterator<ScoreBean> iterator = values.iterator();
		Double sum = 0d;
		// key:zhang values:名字为zhang的所有科目成绩
		// 定义字符串
		String str = key.toString();
		while (iterator.hasNext()) {
			ScoreBean sbBean = iterator.next();
			// 拼接 科目一 分数一 科目二 分数2 科目三 分数3 总成绩
			sum += sbBean.getScore();
			str += " " + sbBean.getSubject() + ":" + sbBean.getScore();
		}
		str += " sum:" + sum;
		context.write(new Text(str), NullWritable.get());
	}

}
