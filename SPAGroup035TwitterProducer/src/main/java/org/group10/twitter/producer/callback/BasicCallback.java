package org.group10.twitter.producer.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class BasicCallback implements Callback{
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception == null) {
			System.out.printf("Offset %d acknowledged at %d\n", metadata.offset(), metadata.partition());
		}
		else {
			System.out.println(exception.getMessage());
		}
	}
}
