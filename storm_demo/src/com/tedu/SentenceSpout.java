package com.tedu;

import java.io.File;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {
	// 模拟一个数据源
	String[] sentence = { "My name is park", "I am so shuai", "do you like me", "are you sure you do not like me",
			"ok i am sure" };
	private SpoutOutputCollector collector = null;
	private int index = 0;

	/**
	 * 调用此方法时，Storm请求喷口向输出收集器发出元组。 这个方法应该是非阻塞的，所以如果喷口没有要发出的元组，这个方法应该返回。
	 * nextuple、ack和fail都在spout任务中的单个线程中的一个紧密循环中调用。
	 * 当没有元组可以发出时，有礼貌的做法是在短时间内（如一毫秒）进行下一次两次睡眠，以免浪费太多CPU。
	 */
	@Override
	public void nextTuple() {
		if (index < sentence.length) {
			collector.emit(new Values(sentence[index]));
			index++;
		} else {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
	}

	/**
	 * 初始化 conf代表当前Topology配置信息 context 上下文 collector
	 * 代表发送者，可以用来发送拓扑，可在任何时候发送，此对象线程安全，可以放心的保存在类的内部作为成员 初始化 conf代表当前Topology配置信息
	 * context 上下文 collector 代表发送者， 可以用来发送拓扑，可在任何时候发送， 此对象线程安全，可以放心的保存在类的内部作为成员
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	/**
	 * 申明输出流的元数据信息 declarer ：输出流的编号，输出的tuple中的字段以及是否是一个指向性的流 组件发送的tuple都要在此声明
	 * 申明输出流的元数据信息 declarer ：输出流的编号，输出的tuple中的字段以及是否是一个指向性的流 组件发送的tuple都要在此声明
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));

	}

}
