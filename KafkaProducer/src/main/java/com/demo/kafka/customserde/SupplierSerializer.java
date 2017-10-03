package com.demo.kafka.customserde;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class SupplierSerializer implements Serializer<Supplier> {

	@Override
	public void close() {
		System.out.println("Closing Supplier Serializer");

	}

	@Override
	public void configure(Map<String, ?> config, boolean isKey) {
		System.out.println("Configuring Supplier Serializer");
		System.out.println("Map is::" + config);
		System.out.println("boolean is:" + isKey);
	}

	@Override
	public byte[] serialize(String topic, Supplier suplr) {

		int sizeOfName = 0;
		int sizeOfDate = 0;
		byte[] serializedName;
		byte[] serializedDate;

		try
		{
			if(suplr == null)
				return null;

			serializedName = suplr.getName().getBytes(StandardCharsets.UTF_8);
			sizeOfName = serializedName.length;

			serializedDate = suplr.getStartDate().toString().getBytes(StandardCharsets.UTF_8);
			sizeOfDate = serializedDate.length;

			/** write data into buffer in a predetermined sequence */
			
			ByteBuffer buf = ByteBuffer.allocate(4+ 4 + sizeOfName + 4 + sizeOfDate);
			buf.putInt(suplr.getId());
			buf.putInt(sizeOfName);
			buf.put(serializedName);
			buf.putInt(sizeOfDate);
			buf.put(serializedDate);

			return buf.array();

		}catch(Exception ex)
		{
			ex.printStackTrace();;
			throw new SerializationException("Exception while serializing Supplier");
		}
	}
}
