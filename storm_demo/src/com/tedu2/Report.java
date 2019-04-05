package com.tedu2;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;

public class Report extends BaseBatchBolt<TransactionAttempt> {
	TransactionAttempt txid = null;

	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this.txid = id;
	}

	Map<String, Integer> map = new HashMap<String, Integer>();

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String word = tuple.getStringByField("word");
		int count = tuple.getIntegerByField("count");
		System.out.println("txid:" + txid.getTransactionId()+"----" + word + "~~~~~~~~~~" + count);
		map.put(word, count);
	}

	@Override
	public void finishBatch() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
