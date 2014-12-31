package bharati.binita.twitter4j.example.streaming.api;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * 
 * @author binita.bharati@gmail.com
 * Demonstration of Twitter streaming API - listens to all public tweets with keyword "Clojure".
 * Puts the tweets into a in-memory LinkedBlockingQueue.
 * 
 * Pre-requisite :
 * A Twitter app should have already been created under your desired twitter id.
 * Once the app is registered , provided all OAuth related info in twitter4j.properties file.
 * 
 *
 */

public class StreamListener {
	
	private static Logger logger = LoggerFactory.getLogger(StreamListener.class);

	
	private LinkedBlockingQueue<Status> queue;

	public LinkedBlockingQueue<Status> getQueue() {
		return queue;
	}

	public void setQueue(LinkedBlockingQueue<Status> queue) {
		this.queue = queue;
	}
	
	public StreamListener(int queueSize)
	{
		this.queue = new LinkedBlockingQueue<>(queueSize);
		
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
				if(queue.size() < 1000)
				{
					logger.info("TWEET Received: " + status);
					queue.offer(status);
				}
				else
				{
					logger.info("Queue is now full, the following message is dropped: "+status);
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
		
		final StreamListener sl = new StreamListener(1000);
		
		Runnable tweetsQReader = new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true)
				{
					Status status = sl.getQueue().peek();
					logger.info("TweetsQReader, got status = "+status);
					if(status == null)
					{
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else
					{
						logger.info("TweetsQReader, got tweet = "+status.getSource());
					}
				}
				
				
				
				
			}
		};
		
		Thread t1 = new Thread(tweetsQReader);
		t1.start();
		
	}

}
