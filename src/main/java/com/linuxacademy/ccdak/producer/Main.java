package com.linuxacademy.ccdak.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {

    public static void main(String[] args) {
       // System.out.println("Hello, world!");
      	Properties props= new Properties();
    	props.put("bootstrap.servers", "localhost:9092");
    	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	props.put("acks", "all");
    	
    	Producer<String,String> producer = new KafkaProducer<String, String>(props);
    	try
    	{
    		File file= new File(Main.class.getClassLoader().getResource("sample_transaction_log.txt").getFile());
    		BufferedReader br = new BufferedReader(new FileReader(file));
    		String line;
    		while(br.readLine() != null)
    		{
    			line=br.readLine();
    			String[] lineArray= line.split(":");
    			String key = lineArray[0];
    			String value= lineArray[1];
    			producer.send(new ProducerRecord<String, String>("inventory_purchases", key, value));
    			if(key.equals("apples"))
    			{
    				producer.send(new ProducerRecord<String, String>("apple_purchases", key, value));
    			} 			
    		}
    		br.close();
    	}
    	catch (IOException e) {
			// TODO: handle exception
    		producer.close();
    		throw new RuntimeException(e);
    		
		}
    	producer.close();
    	
    }

}
