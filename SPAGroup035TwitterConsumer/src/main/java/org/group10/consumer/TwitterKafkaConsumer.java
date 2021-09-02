package org.group10.consumer;

import org.group10.config.KafkaConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;

import org.bson.Document;
import com.mongodb.spark.MongoSpark;

public class TwitterKafkaConsumer {
	
	public static void main(String args[]) throws InterruptedException {
		System.out.println("Spark Streaming starting now");
		
        SparkConf conf = new SparkConf()
                .setAppName("MongoSparkConnection")
                .setMaster("local[*]").
                set("spark.mongodb.output.uri", "mongodb://127.0.0.1/IrGroup10.consumer_data");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));
		
		Map<String, String> kafkaParams  = new HashMap<>();
		kafkaParams.put("metadata.broker.list", KafkaConfig.BOOTSTRAPSERVERS);
		Set<String> topics = Collections.singleton("twitter-feed2");
		
		
		JavaPairInputDStream<String, String> tweets = KafkaUtils.createDirectStream(ssc, 
				  																		  String.class, 
				  																		  String.class, 
				  																		  StringDecoder.class, 
				  																		  StringDecoder.class,
				  																		  kafkaParams, 
				  																		  topics);
		tweets.foreachRDD( rdd -> {			
			JavaRDD<Document> dbEntry = rdd.map( record -> {
				Document doc = new Document();
				doc.put("tweets", record._2.toString());
				return doc;
			});
			try {
				MongoSpark.save(dbEntry);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		});
		
		ssc.start();
		ssc.awaitTermination();
		
		System.out.println("Spark Streaming ended");
	}	
}
