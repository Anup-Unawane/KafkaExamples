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
 *  Also a custom partitioner is used. Messages sent with defined key are
 *  sent to given partitions and other messages are sent to other remaining
 *  partitions.
 *  
 *  Use topic created with 10 partitions.
 *
 */
public class SensorProducer 
{
    public static void main( String[] args )
    {
        final String topic = "SensorTopic";
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("partitioner.class", SensorPartitioner.class.getName());
        props.put("speed.sensor.name", "TSS");
        
        
        Producer<String, String> prod = new KafkaProducer<>(props);
        
        for(int i=0; i< 10; i++)
        {
        	ProducerRecord<String, String> rec = new ProducerRecord<>(topic, "SSP"+ i, "300" + i);
        	prod.send(rec);
        }
        
        for(int i=0; i< 10; i++)
        {
        	ProducerRecord<String, String> rec = new ProducerRecord<>(topic, "TSS", "300" + i);
        	prod.send(rec);
        }
        
        prod.close();
        System.out.println("Done!!!");
    }
}
