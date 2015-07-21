package com.hortonworks.sanne.StromTopologyExample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class StromTopologyExample 
{
	// public static final Logger LOG = LoggerFactory.getLogger(StromTopologyExample.class);
	private static String zkConnString ="localhost:2181";
	private static String topicName = "cobapayments";
	private static Config stormconf;
	
	public static void main( String[] args )
    {
    	TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "kafkaspout", buildKafkaSpout(),2);
    	builder.setBolt( "paymentsjsonserializerbolt", new JSONDeSerializerBolt(),2)
    	.shuffleGrouping("kafkaspout");
    	builder.setBolt("writetohbasebolt", new writeToHbaseBolt(), 2)
    	.shuffleGrouping("paymentsjsonserializerbolt");


    	stormconf = new Config();
	    // stormconf.setDebug(true);
	    String hbaserootdir = "file:///Users/sanne/hbase";
		String zookeeperznodeparent = "localhost:2181";
		// Config stormconfig = new Config();
		
		
		Map<String, Object> hbConfig = new HashMap<String, Object>();
        hbConfig.put("hbase.rootdir", hbaserootdir);
        hbConfig.put("zookeeper.znode.parent", zookeeperznodeparent);
        stormconf.put("hbase.conf", hbConfig);
	    
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("cobapayments", stormconf, builder.createTopology());
		Utils.sleep(60000);
		cluster.killTopology("cobapayments");
		cluster.shutdown();
    	
    }
	
	private static KafkaSpout buildKafkaSpout() {
		
		String zkConnString ="localhost:2181";
		String topicName = "cobapayments";
			
		BrokerHosts hosts = new ZkHosts(zkConnString);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());

//		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//		spoutConfig.forceFromStart = true;
//		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
// private static List<byte[]> messagesReceived = new ArrayList<byte[]>();
		
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		return kafkaSpout;
		
	}
	
	// HBase Bolt is not yet configured right. Prepared for later test - Stephan
	
	private static HBaseBolt buildHbaseBolt() {
		
		
        
		SimpleHBaseMapper mapper = new SimpleHBaseMapper() 
		    .withRowKeyField("id")
		    .withColumnFields(new Fields("SenderName","SenderSurname","SenderBank","SenderIBAN","SenderSWIFT",
					"ReceiverName", "ReceiverSurname", "ReceiverBank", "ReceiverIBAN", "ReceiverSWIFT",
					"AmoutValue", "AmountCurrency", 
					"DescriptionLine1", "DescriptionLine2"))
		    .withColumnFamily("cf1");

			HBaseBolt hbolt = new HBaseBolt("payments", mapper).withConfigKey("hbase.conf");
		
		return hbolt;
	}
	

}







    