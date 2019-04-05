package com.tedu2;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.tuple.Fields;

public class Spout extends BaseTransactionalSpout<MetaData> {

	@Override
	public Coordinator<MetaData> getCoordinator(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return new TranCoordinator();
	}

	@Override
	public Emitter<MetaData> getEmitter(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return new Emitte();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("txid", "sentence"));
	}

}
