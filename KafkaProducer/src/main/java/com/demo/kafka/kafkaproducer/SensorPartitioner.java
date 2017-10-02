package com.demo.kafka.kafkaproducer;

import java.util.Map;
import java.util.List;
import org.apache.kafka.clients.producer.Partitioner;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

/**
 * Custom Partitioner implementation. 
 * 
 *  if sensorName is TSS then messages are sent to 30% partitions i.e. 0,1,2.
 *  <P>
 *  Other messages are sent to remaining partitions 
 *
 */

public class SensorPartitioner implements Partitioner 
{
	private String sensorName;
	
	@Override
	public void configure(Map<String, ?> config) 
	{
		System.out.println("In Configure Method!!");
		sensorName = config.get("speed.sensor.name").toString();
	}

	@Override
	public void close() {
		System.out.println("Closing Partitioner!!");
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, 
					Object value, byte[] valueBytes, Cluster cluster)
	{
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int size = partitions.size();
		
		int speedPartitions =  (int) Math.abs(size * 0.3);
		
		if(keyBytes == null || ! (key instanceof String))
			throw new InvalidRecordException("All messages must have sensor name as key!!");
		
		int partition = 0;
		
		//as key is same i.e.TSS, so partitions are decided using values.
		if(((String)key).equalsIgnoreCase(sensorName))
			partition = Utils.toPositive(Utils.murmur2(valueBytes))% speedPartitions;
		else
			partition = Utils.toPositive(Utils.murmur2(keyBytes)) % (size - speedPartitions) + speedPartitions;
		
		System.out.printf("Key : %s, Partition : %d. \n", key, partition);
		
		return partition;
	}

}
