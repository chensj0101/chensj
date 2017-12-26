package cn.learn.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.learn.storm.bolts.WordCounter;
import cn.learn.storm.bolts.WordNormalizer;
import cn.learn.storm.spouts.WordReader;

public class WordCountApp {

	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-normalizer", new Fields("word"));
		StormTopology topology = builder.createTopology();
		
		Config conf = new Config();
		String fileName = "word.txt";
		conf.put("fileName",fileName);
		conf.setDebug(false);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, topology);
		Thread.sleep(5000);
		cluster.shutdown();
	}
}
