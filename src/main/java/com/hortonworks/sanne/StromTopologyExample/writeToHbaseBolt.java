package com.hortonworks.sanne.StromTopologyExample;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import com.google.protobuf.ServiceException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class writeToHbaseBolt implements IRichBolt{

	private static Configuration config;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = Logger.getLogger(writeToHbaseBolt.class);
	
	protected OutputCollector collector;
	
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		// Configure Connection
				config = HBaseConfiguration.create();
				// config.set("hbase.zookeeper.quorum", "172.16.97.157");  // sandbox or cluster
				config.set("hbase.zookeeper.quorum", "localhost");
				config.set("hbase.zookeeper.property.clientport", "2181");
		        // config.set("zookeeper.znode.parent", "/hbase-unsecure"); // sandbox or cluster //this is what most people miss :)
		        try {
					HBaseAdmin.checkHBaseAvailable(config);
				} catch (ServiceException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MasterNotRunningException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ZooKeeperConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	}
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple inputTuple) {
		// TODO Auto-generated method stub
		System.out.println("Hbasebolt: " + inputTuple);
		// hbolt.execute(inputTuple);
		
		byte[] tablename = Bytes.toBytes("payments");
		@SuppressWarnings("resource")
		HTable table = null;
		try {
			table = new HTable(config, tablename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] rowkey1 = Bytes.toBytes(inputTuple.getString(0));
		byte[] columnfamily = Bytes.toBytes("cf1");
		
		Put p1 = new Put(rowkey1);
		Put p2 = new Put(rowkey1);
		Put p3 = new Put(rowkey1);
		Put p4 = new Put(rowkey1);
		Put p5 = new Put(rowkey1);
		Put p6 = new Put(rowkey1);
		Put p7 = new Put(rowkey1);
		Put p8 = new Put(rowkey1);
		Put p9 = new Put(rowkey1);
		Put p10 = new Put(rowkey1);
		Put p11 = new Put(rowkey1);
		Put p12 = new Put(rowkey1);
		Put p13 = new Put(rowkey1);
		Put p14 = new Put(rowkey1);
		
		
//		"SenderName","SenderSurname","SenderBank","SenderIBAN","SenderSWIFT",
//		"ReceiverName", "ReceiverSurname", "ReceiverBank", "ReceiverIBAN", "ReceiverSWIFT",
//		"AmoutValue", "AmountCurrency", 
//		"DescriptionLine1", "DescriptionLine2"
		
		p1.add(columnfamily, Bytes.toBytes("SenderName"), Bytes.toBytes(inputTuple.getString(1)));
		p2.add(columnfamily, Bytes.toBytes("SenderSurname"), Bytes.toBytes(inputTuple.getString(2)));
		p3.add(columnfamily, Bytes.toBytes("SenderBank"), Bytes.toBytes(inputTuple.getString(3)));
		p4.add(columnfamily, Bytes.toBytes("SenderIBAN"), Bytes.toBytes(inputTuple.getString(4)));
		p5.add(columnfamily, Bytes.toBytes("SenderSWIFT"), Bytes.toBytes(inputTuple.getString(5)));
		p6.add(columnfamily, Bytes.toBytes("ReceiverName"), Bytes.toBytes(inputTuple.getString(6)));
		p7.add(columnfamily, Bytes.toBytes("ReceiverSurname"), Bytes.toBytes(inputTuple.getString(7)));
		p8.add(columnfamily, Bytes.toBytes("ReceiverBank"), Bytes.toBytes(inputTuple.getString(8)));
		p9.add(columnfamily, Bytes.toBytes("ReceiverIBAN"), Bytes.toBytes(inputTuple.getString(9)));
		p10.add(columnfamily, Bytes.toBytes("ReceiverSWIFT"), Bytes.toBytes(inputTuple.getString(10)));
		p11.add(columnfamily, Bytes.toBytes("AmountValue"), Bytes.toBytes(inputTuple.getString(11)));
		p12.add(columnfamily, Bytes.toBytes("AmountCurrency"), Bytes.toBytes(inputTuple.getString(12)));
		p13.add(columnfamily, Bytes.toBytes("DescriptionLine1"), Bytes.toBytes(inputTuple.getString(13)));
		p14.add(columnfamily, Bytes.toBytes("DescriptionLine2"), Bytes.toBytes(inputTuple.getString(14)));
		
		try {
			table.put(p1);
			table.put(p2);
			table.put(p3);
			table.put(p4);
			table.put(p5);
			table.put(p6);
			table.put(p7);
			table.put(p8);
			table.put(p9);
			table.put(p10);
			table.put(p11);
			table.put(p12);
			table.put(p13);
			table.put(p14);
			
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		
		
	}

	

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
