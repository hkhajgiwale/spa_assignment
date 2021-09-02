package org.group10.twitter;

import org.group10.twitter.producer.TwitterKafkaProducer;

public class App {
	public static void main(String[] args) {
		System.out.println("TWEET TWEET");
		TwitterKafkaProducer producer = new TwitterKafkaProducer();
		producer.run();
	}
}
