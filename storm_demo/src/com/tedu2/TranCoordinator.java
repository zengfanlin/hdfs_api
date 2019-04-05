package com.tedu2;

import java.math.BigInteger;

import backtype.storm.transactional.ITransactionalSpout.Coordinator;

public class TranCoordinator implements Coordinator<MetaData> {
	boolean hasMore = true;
	int batchsize = 3;

	/*
	 * 当isready为true的时候，storm会调用此方法来真正获取一个批的信息 传入批的编号和上一个批的元数据信息要求返回当前批的元数据信息
	 * 
	 * @see backtype.storm.transactional.ITransactionalSpout.Coordinator#
	 * initializeTransaction(java.math.BigInteger, java.lang.Object)
	 */
	@Override
	public MetaData initializeTransaction(BigInteger txid, MetaData prevMetadata) {
		int startrow = prevMetadata == null ? 0 : prevMetadata.getEndRow();
		int stoprow = (Db.getData().length - startrow) > batchsize ? startrow + batchsize : Db.getData().length;
		hasMore = stoprow != Db.getData().length;
		return new MetaData(startrow, stoprow);
	}

	/*
	 * 如果发出的OK为Start a New Transaction，则返回true，否则返回false（将跳过此事务）。
	 * 如果您希望在请求下一个事务（这将在循环中重复调用）之间有一个延迟，那么您应该在这里睡觉。 询问是否发送一个批
	 * 如果已经准备好则为true，否则为false. 如果希望延迟发送，可以在这个方法中，sleep一会。 此方法会在循环中不停调用
	 * 
	 * @see backtype.storm.transactional.ITransactionalSpout.Coordinator#isReady()
	 */
	@Override
	public boolean isReady() {
		// TODO Auto-generated method stub
		return hasMore;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
