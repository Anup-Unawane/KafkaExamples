package com.demo.kafka.customserde;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SupplierConsumer 
{
	public static void main(String[] args) 
	{
		String topicName = "SupplierTopic";
		String topicGroup = "SupplierGroup";
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", topicGroup);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", SupplierDeserializer.class.getName());
		
		KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));
		
		while(true)
		{
			ConsumerRecords<String, Supplier> records = consumer.poll(100);
			for(ConsumerRecord<String, Supplier> rec : records)
			{
				System.out.println("\n**************************************\n");
				System.out.println("Key::" + rec.key());
				Supplier sup = rec.value();
				System.out.println("Supplier ID::" + sup.getId());
				System.out.println("Supplier Name:" + sup.getName());
				System.out.println("Supplier Date::" + sup.getStartDate());
				System.out.println("\n**************************************\n");
			}
		}
	}
}
