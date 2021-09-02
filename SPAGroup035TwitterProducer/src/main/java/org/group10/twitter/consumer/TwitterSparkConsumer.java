package org.group10.twitter.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.group10.twitter.config.KafkaConfig;

import kafka.serializer.StringDecoder;

public class TwitterSparkConsumer {
	
	public static void main(String args[]) throws InterruptedException {
		System.out.println("Spark Streaming starting now");
		//SparkConf conf = new SparkConf().setAppName("KafkaSparkConsumer").setMaster("spark://DDSPL1724:7077");

        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));
//		SparkConf conf = new SparkConf().setAppName("KafkaSparkConsumer")
//										.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
//										.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection");
//		
		
		Map<String, String> kafkaParams  = new HashMap<>();
		kafkaParams.put("metadata.broker.list", KafkaConfig.BOOTSTRAPSERVERS);
		Set<String> topics = Collections.singleton(KafkaConfig.TOPIC);
		
		
		JavaPairInputDStream<String, String> tweets = KafkaUtils.createDirectStream(ssc, 
				  																		  String.class, 
				  																		  String.class, 
				  																		  StringDecoder.class, 
				  																		  StringDecoder.class,
				  																		  kafkaParams, 
				  																		  topics);

        tweets.foreachRDD(rdd -> {
            System.out.println("--- Received new data RDD  " + rdd.partitions().size() + " partitions and " + rdd.count() + " records");
            //rdd.
            rdd.foreach(record -> System.out.println(record._2));
        });
		
      
//		JavaPairDStream<String, String> results = tweets.mapToPair( record -> new Tuple2<>(record.key(), record.value()));
//		
//		SparkSession spark =  SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
//																	.config("spark.mongodb.input.uri", "mongodb:127.0.0.1/test/myCollection")
//																	.config("saprk.mongodb.output.uri", "mongodb:127.0.0.1/test/myCollection")
//																	.getOrCreate();
//		
		
		
		ssc.start();
		ssc.awaitTermination();
		
		System.out.println("Spark Streaming ended");
	}	
}
