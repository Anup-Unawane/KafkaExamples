package com.demo.kafka.customserde;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class SupplierDeserializer implements Deserializer<Supplier> {

	@Override
	public void close() {
		System.out.println("Closing Supplier Deserializer");

	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		System.out.println("Configuring Supplier Deserializer");
		System.out.println("Map is::" + configs);
		System.out.println("boolean is:" + isKey);

	}

	@Override
	public Supplier deserialize(String topic, byte[] data) 
	{
		if(data == null)
		{
			System.out.println("Null data received!!!");
			return null;
		}

		/** read data from buffer in a predetermined sequence as written in serializer*/
		
		try
		{
			ByteBuffer buf = ByteBuffer.wrap(data);
			int id = buf.getInt();

			int sizeOfName = buf.getInt();
			byte[] nameBytes = new byte[sizeOfName];
			buf.get(nameBytes);
			String supName = new String(nameBytes, StandardCharsets.UTF_8);

			int sizeOfDate = buf.getInt();
			byte[] dateBytes = new byte[sizeOfDate];
			buf.get(dateBytes);
			String startDate = new String(dateBytes, StandardCharsets.UTF_8);

			DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

			return new Supplier(id,supName,df.parse(startDate));
		}catch(Exception ex)
		{
			ex.printStackTrace();
			throw new SerializationException("Exception occurred while deserializing supplier data");
		}

	}

}
