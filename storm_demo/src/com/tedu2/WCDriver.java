package com.tedu2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;

public class WCDriver {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception, InvalidTopologyException {
		// 创建组件
		Spout spout = new Spout();
		SpBolt splitBolt = new SpBolt();
		WCBolt wcBolt = new WCBolt();
		Report reportBolt = new Report();
		// 拓扑构建者
		
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("tran_top", "tran_sp", spout);

		builder.setBolt("sp_bolt", splitBolt).shuffleGrouping("tran_sp");
//		builder.setCommitterBolt("wc_bolt", wcBolt).fieldsGrouping("sp_bolt", new Fields("word"));
		builder.setBolt("wc_bolt", wcBolt).fieldsGrouping("sp_bolt", new Fields("word"));
		builder.setBolt("re_bolt", reportBolt).globalGrouping("wc_bolt");
		// 通过构建者创建拓扑
		StormTopology topology = builder.buildTopology();
//		Config config = new Config();
//		StormSubmitter.submitTopology("Tans_topo", config, topology);

		LocalCluster cluster = new LocalCluster();
		Config config = new Config();
		cluster.submitTopology("tran_top", config, topology);
		Thread.sleep(1000 * 10);
		cluster.killTopology("tran_top");
		cluster.shutdown();
	}

}
