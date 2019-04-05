package com.tedu2;

import java.math.BigInteger;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout.Emitter;
import backtype.storm.tuple.Values;
import backtype.storm.transactional.TransactionAttempt;

public class Emitte implements Emitter<MetaData> {

	/**
	 * 真正发送批的方法，根据传入的事务编号和元数据信息组织批来发送
	 * 传入的元数据信息就是在Coordinator的initializeTransaction方法中返回的数据 要注意对于同一个tx
	 * 要保证每次发送的都是相同的tuple以便于在数据重发时保持数据的一致 批中所有tuple 的第一个字段必须是tx
	 */
	@Override
	public void emitBatch(TransactionAttempt tx, MetaData metaData, BatchOutputCollector collector) {
		int start = metaData.getStartRow();
		int stop = metaData.getEndRow();
		for (int i = start; i < stop; i++) {
			String sentences = Db.getData()[i];
			collector.emit(new Values(tx, sentences));
		}
	}

	/**
	 * 当storm对某个批的数据都完成了完整的处理后，会调用此方法来通知，在此方法中可以用来清理之前为重发而缓存的数据
	 */
	@Override
	public void cleanupBefore(BigInteger txid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
