package com.hortonworks.sanne.StromTopologyExample;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JSONDeSerializerBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	OutputCollector _collector;
	HashMap tupleHash = null;
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
	
	public void declareOutputFields(OutputFieldsDeclarer _declarer) {
		_declarer.declare(new Fields("id","senderName"));
	}

	public void execute(Tuple _tuple) {
		String kafkaString = null;
		
		System.out.println("Tuple: " + _tuple);
		Object value = _tuple.getValue(0);
		byte[] kafkaBytes = (byte[]) value;
		try {
			kafkaString = new String(kafkaBytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		
		try {
			// JSONDeSerializer(kafkaString);
			tupleHash = JSONtoHashMap(kafkaString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Tuple Deserializer: " + tupleHash);
		// System.out.println("Message: \n" + kafkaString);
		// _collector.ack(_tuple);
		String id = tupleHash.get("id").toString();
		String senderName = tupleHash.get("SenderName").toString();
		// System.out.println("Id-Text: " + test);
		_collector.emit(new Values(id,senderName));
	}
	

	public void cleanup() {
		// TODO Auto-generated method stub

	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	} 
	

	private HashMap JSONtoHashMap(String jsonString) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonString);
		
		JSONObject jsonSender = (JSONObject) jsonObject.get("sender");
		JSONObject jsonReceiver = (JSONObject) jsonObject.get("receiver");
		JSONObject jsonAmount = (JSONObject) jsonObject.get("amount");
		JSONObject jsonDescription = (JSONObject) jsonObject.get("description");
		
		HashMap<String, String> hMap = new HashMap<String, String>();
		
		hMap.put("id", jsonObject.get("id").toString());
		
		hMap.put("SenderName", jsonSender.get("name").toString());
		hMap.put("SenderSurname", jsonSender.get("surname").toString());
		hMap.put("SenderBank", jsonSender.get("bank").toString());
		hMap.put("SenderIBAN", jsonSender.get("IBAN").toString());
		hMap.put("SenderSWIFT", jsonSender.get("SWIFT").toString());
		
		hMap.put("receiverName", jsonReceiver.get("name").toString());
		hMap.put("receiverSurname", jsonReceiver.get("surname").toString());
		hMap.put("receiverBank", jsonReceiver.get("bank").toString());
		hMap.put("receiverIBAN", jsonReceiver.get("IBAN").toString());
		hMap.put("receiverSWIFT", jsonReceiver.get("SWIFT").toString());
		
		hMap.put("amoutValue", jsonAmount.get("value").toString());
		hMap.put("amountCurrency", jsonAmount.get("currency").toString());
		
		hMap.put("descriptionLine1", jsonDescription.get("line1").toString());
		hMap.put("descriptionLine2", jsonDescription.get("line2").toString());
		
		
		return hMap;
	}
	
	private void JSONDeSerializer(String jsonString) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonString);
		
		// System.out.println("Transaction ID: " + jsonObject.get("id"));
		long transId = Long.valueOf(jsonObject.get("id").toString());
		System.out.println("Transaction ID: " + transId + "\n");
		
		// handle a structure into the json object
		System.out.println("Sender:");
		System.out.println("========");
		JSONObject sender = (JSONObject) jsonObject.get("sender");
		
		String senderName = sender.get("name").toString();
		String senderSurname = sender.get("surname").toString();
		String senderBank = sender.get("bank").toString();
		String senderIBAN = sender.get("IBAN").toString();
		String senderSWIFT = sender.get("SWIFT").toString();
		
		System.out.println("Name: " + senderName);
		System.out.println("Surname: " + senderSurname);
		System.out.println("Bank: " + senderBank);
		System.out.println("IBAN: " + senderIBAN);
		System.out.println("SWIFT: " + senderSWIFT);
			
		System.out.println("\nReceiver:");
		System.out.println("===========");
		JSONObject receiver = (JSONObject) jsonObject.get("receiver");
		
		String receiverName = receiver.get("name").toString();
		String receiverSurname = receiver.get("surname").toString();
		String receiverBank = receiver.get("bank").toString();
		String receiverIBAN = receiver.get("IBAN").toString();
		String receiverSWIFT = receiver.get("SWIFT").toString();
		
		System.out.println("Name: " + receiverName);
		System.out.println("Surname: " + receiverSurname);
		System.out.println("Bank: " + receiverBank);
		System.out.println("IBAN: " + receiverIBAN);
		System.out.println("SWIFT: " + receiverSWIFT);
		
		System.out.println("\nAmount:");
		System.out.println("========= ");
		JSONObject amount = (JSONObject) jsonObject.get("amount");
		
		Float amountValue = Float.valueOf(amount.get("value").toString());
		String amountCurrency = amount.get("currency").toString();
		
		System.out.println("Value: " + amountValue);
		System.out.println("Currency: " + amountCurrency);
		
		System.out.println("\nDescription:");
		System.out.println("============== ");
		JSONObject description = (JSONObject) jsonObject.get("description");
		
		String descriptionLine1 = description.get("line1").toString();
		String descriptionLine2 = description.get("line2").toString();
		
		System.out.println("Line1: " + descriptionLine1);
		System.out.println("Line2: " + descriptionLine2);
		
		
	}

	

}
