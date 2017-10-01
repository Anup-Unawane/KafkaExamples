package com.demo.kafka.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Simple Kafka Producer. 
 * This is an example for Synchronous Mechanism for Kafka Producer.
 * Here, producer waits for response to receive from broker. 
 *
 */
public class SynchronousKafkaProducer 
{
	public static void main( String[] args )
	{
		final String topic = "MyFirstTopic1";
		final String key = "Key2";
		final String value = "Value2";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		Producer<String, String> prod = new KafkaProducer<>(props);

		try
		{
			for(int i=0; i< 10; i++)
			{
				ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, value + "_" + i);
				RecordMetadata recMD = prod.send(rec).get();
				System.out.printf("Message is sent to Partition:%d, and offset:%d. \n", recMD.partition(), recMD.offset());
			}
		}catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println("Exception occurred while sending message to broker." + e.getMessage());
		}finally
		{
			prod.close();
		}
		System.out.println("Done!!!");
	}
}
