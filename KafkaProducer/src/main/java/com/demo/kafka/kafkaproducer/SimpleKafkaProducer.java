package com.demo.kafka.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Simple Kafka Producer. 
 * This is an example for Send and Forget Mechanism for Kafka Producer.
 * Here, producer does not wait for response to receive from broker. 
 *
 */
public class SimpleKafkaProducer 
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
        
        for(int i=0; i< 10; i++)
        {
        	ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, value + "_" + i);
        	prod.send(rec);
        }
        prod.close();
        System.out.println("Done!!!");
    }
}
