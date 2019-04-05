package com.tedu2;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * implements ICommitter 变成commit阶段的bolt,finishBatch()严格按照顺序执行的bolt
 * 或者通过设置setCommitbolt方法
 * @author Administrator
 *
 */
public class WCBolt extends BaseBatchBolt<TransactionAttempt> implements ICommitter {
	private TransactionAttempt txid = null;
	private BatchOutputCollector collector = null;

	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this.txid = id;
		this.collector = collector;
	}

	Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
	// 需要保存有序并且不重复的单词
	private Set<String> set = new LinkedHashSet<>();

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		map.put(word, map.containsKey(word) ? map.get(word) + 1 : 1);
		set.add(word);
	}
	//在多个node 多个worker中，这个参数会有并发问题，不 能用，可以将数据保存在redis或者zk中
	//单机环境下，设置worker没有作用，还是以一个worker运行，所以没有问题
	private static int lastTranId=0;
	@Override
	public void finishBatch() {
		if(txid.getTransactionId().intValue()<=lastTranId) {
			//当前， 的批次小于等于最后处理的编号，说明是重发的数据，要抛弃
			return;
		}
		else {
			//处理批次大的数据
			for (String word : set) {
				collector.emit(new Values(txid, word, map.get(word)));
			}
			lastTranId=txid.getTransactionId().intValue();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("txid", "word","count"));
	}

}
