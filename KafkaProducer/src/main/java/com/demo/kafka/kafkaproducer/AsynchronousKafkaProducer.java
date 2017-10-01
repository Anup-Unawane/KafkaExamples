package com.demo.kafka.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Asynchronous Kafka Producer. 
 * This is an example for Asynchronous Mechanism for Kafka Producer.
 * Here, you provide a callback mechanism for producer. After each
 * message is sent to broker, this callback mechanism is called with
 * {@link RecordMetadata} and Exception object.
 * 
 * This mechanism gives throughput as good as Fire and Forget Mechanism.
 * However, you have limit of inflight messages.This limit is controlled by configuration
 * parameter: max.in.flight.requests.per.connection
 * This represents number of messages you can send to server without 
 * receiving response. Defaults to 5
 * 
 *
 */
public class AsynchronousKafkaProducer 
{
	public static void main( String[] args )
	{
		final String topic = "MyFirstTopic1";
		final String key = "Key2";
		final String value = "Async_Value2";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		Producer<String, String> prod = new KafkaProducer<>(props);

		for(int i=0; i< 10; i++)
		{
			ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, value + "_" + i);
			prod.send(rec, new MyProducerCallback());
		}
		prod.close();
		System.out.println("Done!!!");
	}
}

class MyProducerCallback implements Callback
{

	@Override
	public void onCompletion(RecordMetadata recMD, Exception ex) 
	{
		if(ex != null)
			System.out.println("Exception has occurred." + ex.getMessage());
		else
			System.out.println("Asynchronous Call Success.");
	}

}
