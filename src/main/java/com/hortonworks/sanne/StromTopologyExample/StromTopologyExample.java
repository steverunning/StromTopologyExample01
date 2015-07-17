package com.hortonworks.sanne.StromTopologyExample;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
import backtype.storm.utils.Utils;

public class StromTopologyExample 
{
	// public static final Logger LOG = LoggerFactory.getLogger(StromTopologyExample.class);
	private static String zkConnString ="localhost:2181";
	private static String topicName = "cobapayments";
	
	public static void main( String[] args )
    {
    	TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "kafkaspout", buildKafkaSpout(),2);
    	builder.setBolt( "paymentsjsonserializerbolt", new JSONDeSerializerBolt(),2)
    	.shuffleGrouping("kafkaspout");
    	builder.setBolt("writetohbasebolt", new writeToHbaseBolt(), 2)
    	.shuffleGrouping("paymentsjsonserializerbolt");


		Config conf = new Config();
	    // conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("cobapayments", conf, builder.createTopology());
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
	

}







    