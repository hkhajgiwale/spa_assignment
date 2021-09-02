package org.group10.twitter.producer;


import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.group10.twitter.config.KafkaConfig;
import org.group10.twitter.config.TwitterConfig;
import org.group10.twitter.model.Tweet;
import org.group10.twitter.model.User;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {
	private Client client;
	private BlockingQueue<String> queue;
	private Gson gson;
	
	public TwitterKafkaProducer() {
		Authentication authentication = new OAuth1(
				TwitterConfig.CONSUMER_KEY,
				TwitterConfig.CONSUMER_SECRET,
				TwitterConfig.ACCESS_TOKEN,
				TwitterConfig.ACCESS_SECRET
				);
	
		StatusesFilterEndpoint endpoint =  new StatusesFilterEndpoint();
		endpoint.trackTerms(Collections.singletonList(TwitterConfig.TERM));
		
		queue = new LinkedBlockingQueue<>(1000);
		
		client = new ClientBuilder()
				 	.hosts(Constants.STREAM_HOST)
				 	.authentication(authentication)
				 	.endpoint(endpoint)
				 	.processor(new StringDelimitedProcessor(queue))
				 	.build();
		
		gson = new Gson();
	}
	//org.apache.kafka.common.serialization.LongSerializer

	private Producer<Long, String> getProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVERS);
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
	
	@SuppressWarnings("unchecked")
	public void run() {
		client.connect();
		try (Producer<Long, String> producer = getProducer()) {
			while(true) {
				Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
				//User user = gson.fromJson(queue.take(), User.class);
				System.out.printf("Tweet ID: %d\n", tweet.getId());
				
				long tweetId = tweet.getId();
				String tweetText = tweet.getText();
				String tweetLang = tweet.getLang();
				User user = tweet.getUser();
				
				long userId = user.getId();
				String userName = user.getName();
				String userScreenName = user.getScreenName();
				String userLocation = user.getLocation();
				int userFollowersCount = user.getFollowersCount();
				
				JSONObject obj = new JSONObject();
				obj.put("tweet_id", tweetId);
				obj.put("tweet_text", tweetText );
				obj.put("tweet_lang", tweetLang);
				obj.put("user_id", userId);
				obj.put("user_name", userName);
				obj.put("user_screen_name", userScreenName);
				obj.put("user_location", userLocation);
				obj.put("user_followers_count", userFollowersCount);
			
				ProducerRecord<Long, String> record = new ProducerRecord<Long, String>("twitter-feed2", tweetId, obj.toString());
				
				producer.send(record, (metadata, exception) -> {
					if (metadata != null) {
						System.out.println(record.key() + " : " + record.value());
					}
					else {
						System.out.println("Error sending records ->" + record.value());
					}
				});
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			client.stop();
		}
	}
}
