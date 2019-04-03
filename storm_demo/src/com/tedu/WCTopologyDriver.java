package com.tedu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WCTopologyDriver {
	public static void main(String[] args) throws Exception, InvalidTopologyException {
		// 创建组件
		SentenceSpout sentenceSpout = new SentenceSpout();
		SentenceBolt sentenceBolt = new SentenceBolt();
		WordCountBolt wpBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		// 拓扑构建者
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sentence_Spout", sentenceSpout);
		builder.setBolt("sentence_Bolt", sentenceBolt).shuffleGrouping("sentence_Spout");
		builder.setBolt("WordCount_Bolt", wpBolt).fieldsGrouping("sentence_Bolt", new Fields("word"));
		builder.setBolt("Report_Bolt", reportBolt).globalGrouping("WordCount_Bolt");
		// 通过构建者创建拓扑
		StormTopology topology = builder.createTopology();
//		Config config = new Config();
//		StormSubmitter.submitTopology("WC_StormTopology", config, topology);
		
		LocalCluster cluster=new LocalCluster();
		Config config = new Config();
		cluster.submitTopology("WC_StormTopology", config, topology);
		Thread.sleep(10000);
		cluster.killTopology("WC_StormTopology");
		cluster.shutdown();
	}

}
