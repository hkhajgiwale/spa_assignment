package org.group10.twitter.config;

public class KafkaConfig {
	public static final String BOOTSTRAPSERVERS = "localhost:9092";
	public static final String TOPIC = "twitter-feed";
	public static final String MAX_IN_FLIGHT_CONN = "5";
	
//	public static final String COMPRESSION_TYPE = "snappy";
//	public static final String RETRIES_CONFIG = Integer.toString(Integer.MAX_VALUE);
//	public static final String BATCH_SIZE = Integer.toString(32 * 1024);
}
