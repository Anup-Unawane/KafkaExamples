package com.demo.kafka.customserde;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SupplierProducer 
{
	public static void main(String[] args) throws ParseException 
	{
		final String topic = "SupplierTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", SupplierSerializer.class.getName());

		Producer<String, Supplier> prod = new KafkaProducer<>(props);

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Supplier sp1 = new Supplier(101,"XYZ Pvt Ltd.",df.parse("2016-04-01"));
		Supplier sp2 = new Supplier(102,"ABC Pvt Ltd.",df.parse("2012-01-01"));

		prod.send(new ProducerRecord<>(topic, "101", sp1));
		prod.send(new ProducerRecord<>(topic, "102", sp2));

		prod.close();
		System.out.println("SupplierProducer Done!!!");
	}
}
