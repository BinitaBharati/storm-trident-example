package bharati.binita.twitter4j.example.streaming.api;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import redis.clients.jedis.Jedis;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;

/**
 * 
 * @author binita.bharati@gmail.com
 * Demonstration of Twitter streaming API - listens to all public tweets with keyword "Clojure".
 * Puts the tweets into redis.
 * 
 * Pre-requisite :
 * 1) A Twitter app should have already been created under your desired twitter id.
 * Once the app is registered , provided all OAuth related info in twitter4j.properties file.
 * 
 * 2)Redis server should be up and running.
 * 
 *
 */

public class StreamListener2 {
	
	private static Logger logger = LoggerFactory.getLogger(StreamListener.class);
	
	private static Jedis jedis;


	public StreamListener2(String redisHost, String redisPort)
	{
		
		jedis = new Jedis(redisHost, Integer.valueOf(redisPort));
		
		StatusListener listener = new StatusListener() {
			
			@Override
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onStatus(Status status) {
				// TODO Auto-generated method stub
				
					logger.info("onStatus: TWEET Received: " + status);
					if(status != null)
					{
						URLEntity[] urls = status.getURLEntities();
						logger.info("onStatus: urls: " + urls.length);

				        for(int i = 0; i < urls.length;i++){
				          jedis.rpush("url", urls[i].getURL().trim());
				        }
					}
					
				
			}
			
			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}
		};
		
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        FilterQuery filter = new FilterQuery();
        filter.count(0);
        String[] trackTerms = new String[]{"Clojure"};
        filter.track(trackTerms);
        twitterStream.filter(filter);
	}
	
	public static void main(String[] args) {
		
		final StreamListener2 sl = new StreamListener2("127.0.0.1","6379");
		
		Runnable urlReader = new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				//while(true)
				//{
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					String url = jedis.rpop("url");
					logger.info("UrlReader: url = "+url);
				//}
				
				
				
				
			}
		};
		
		Thread t1 = new Thread(urlReader);
		t1.start();
		
	}


}
