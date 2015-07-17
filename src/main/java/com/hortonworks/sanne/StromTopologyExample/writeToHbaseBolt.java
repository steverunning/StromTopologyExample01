package com.hortonworks.sanne.StromTopologyExample;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class writeToHbaseBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple inputTuple) {
		// TODO Auto-generated method stub
		System.out.println("Hbasebolt: " + inputTuple);
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
