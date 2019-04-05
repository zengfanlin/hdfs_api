package com.tedu2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.string__init;

public class SpBolt extends BaseBatchBolt<TransactionAttempt> {
	BatchOutputCollector collector = null;
	TransactionAttempt txid = null;

	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this.collector = collector;
		txid = id;
	}

	private List<String> list = new ArrayList<String>();

	/**
	 * 处理批中所有tuple
	 */
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		list.addAll(Arrays.asList(words));
	}

	/**
	 * 批中所有数据处理完成后，调用此方法
	 */
	@Override
	public void finishBatch() {
		for (String word : list) {
			collector.emit(new Values(txid, word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("txid", "word"));
	}

}
