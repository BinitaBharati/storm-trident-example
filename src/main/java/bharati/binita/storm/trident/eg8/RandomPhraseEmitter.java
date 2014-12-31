package bharati.binita.storm.trident.eg8;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import bharati.binita.storm.trident.util.CommonUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

/**
 * 
 * @author binita.bharati@gmail.com
 * Responsible for emitting batches of tuples per Spout trxn.
 * If batch replay feature is required, then, its upto the API user to persist
 * the trxn to batch data mapping.
 * 
 *
 * 
 * Do not have static fields in your Storm/Trident components:
 * https://groups.google.com/forum/#!topic/storm-user/Dlpg3G8rx7w
 *
 */

public class RandomPhraseEmitter implements Emitter<String>, Serializable{//Object is the type of the tuple that will be emitted
	
	private static Logger logger = LoggerFactory.getLogger(RandomPhraseEmitter.class);
	private static Random rand = new Random();
	private static int latestIndex = 0;//wrong to have this in a distributd system - use some storage layer to achieve the same
	private int phraseCount;
		
	private String[] sentences;
	
	private RedisOperations redisOperations;

		
	public RandomPhraseEmitter(Integer count, String redisServerIP, String redisServerPort)
	{
		this.phraseCount = count;
			
        this.redisOperations = new RedisOperations(redisServerIP, redisServerPort);
        
		try {
			sentences = (String[]) IOUtils.readLines(
			    ClassLoader.getSystemClassLoader().getResourceAsStream("500_sentences_en.txt")).toArray(new String[0]);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
	}

	 /**
     * Emit a batch for the specified transaction attempt and metadata for the transaction. The metadata
     * was created by the Coordinator in the initializeTranaction method. Unlike transactional spout,
     * an opaque transactional spout cannot guarantee that the batch of tuples for a txid remains constant..
     * 
     * emitBatch will keep getting called repeatedly. Each invocation to emitBatch will be done in a new trxn.
     * emitBatch is internally invoked by Storm repeatedly, like a infinite while loop.Each iteration of the
     * loop is a trxn.Trxn Ids are auto-generated.
     * 
     */

	@Override
	public void emitBatch(TransactionAttempt trxnAttempt, String coordinatorMeta,
			TridentCollector collector) {
		// TODO Auto-generated method stub
		
		String curThreadName = Thread.currentThread().getName();
		/* During batch replay scenario, failed trxId will be repeated. i.e you will see the same trxId more than once.
		 * But, success method for the failed trxn gets invoked immediately, so , basically, you can NOT use the same trxn id
		 * more than once in Opaque Transactional Spout. If you want to replay older data, use a new trxn id.
		 * 
		 * Consequently, and also in contrast to Transactional Spouts, Opaque Transactional Spouts do not require every batch data to be persisted (in order to replay).
		 * Only the failed batch data should be persisted, unlike Transactional Spouts.In Transactional Spouts, even the successful trxns have to be persisted beforehand;
		 * for replay in case of subsequent failure during further processing.
		 *
		 */
		long trxnId = trxnAttempt.getTransactionId();
		CommonUtil.logMessage(logger, curThreadName, "emitBatch: entered with latestIndex = %d",  latestIndex);


		CommonUtil.logMessage(logger, curThreadName, "emitBatch: entered with trxnId = %d",  trxnId);
		
		//Check if any previous trxn has failed.If yes, replay the failed trxn's data first. Check RedisStoreIBackingMap:multiPut.
		List<String> prevFailedData = redisOperations.lrange("replayPhrase", 0, 1);
			CommonUtil.logMessage(logger, curThreadName, "emitBatch: prevFailedData = %s",  prevFailedData);
			
		String randomPhrase = null;
			
		if(prevFailedData != null && prevFailedData.size() > 0)//Implies a earlier trxn had failed, need to replay that data.
		{
			randomPhrase = prevFailedData.get(0);
			//Now, remove the failedData from Redis.
			redisOperations.lpop("replayPhrase", 1);
		}
		//No previously failed data
		/**
		 *  2014-11-28 21:07:16 b.s.util [ERROR] Async loop died!
               java.lang.RuntimeException: java.lang.ArrayIndexOutOfBoundsException: 498
            2014-11-28 21:07:16 b.s.util [INFO] Halting process: ("Worker died")
            014-11-28 21:13:43 b.s.d.worker [INFO] Launching worker for   trident-eg5-2-1417188174 on 223733d0-502e-4bfd-a074-ca39e7d5c130:6703 with  id e4c3658f-881b-493b-8304-65bc01ddf18a and conf {"dev.zookeeper.path"  "/tmp/dev-storm-zookeeper".......................

		 */
		else if(latestIndex < 498)//To stop java.lang.ArrayIndexOutOfBoundsException , and to prevent worker from dying and restarting.
		{
			CommonUtil.logMessage(logger, curThreadName, "emitBatch: trxn = %s is a fresh one.",  trxnId);
			
			randomPhrase = sentences[latestIndex];
			
			latestIndex++;
			
			
		}
		CommonUtil.logMessage(logger, curThreadName, "emitBatch: randomPhrase = %s, trxnId = %s", randomPhrase, trxnId);
		
		List<Object> randomPhraseList = new ArrayList<>();
		randomPhraseList.add(randomPhrase);
		
		collector.emit(randomPhraseList);
	
			
	}

	 /**
     * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
     * In case of batch replay scenario in Opaque Transactional State Management, this method will get invoked
     * (irrespective of thrown FailedException for a trxnId). This is because as per design of Opaque 
     * Transactional State Management, the same data isn't tied to a partuclar trxnId.
     * The same failed data can be transmitted via another trxnId.
     */

	@Override
	public void success(TransactionAttempt trxnAttempt) {
		// TODO Auto-generated method stub
		String curThreadName = Thread.currentThread().getName();
		try {
				throw new Exception("success: throwing intentional Exception - check code flow");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		long trxnId = trxnAttempt.getTransactionId();
		
		CommonUtil.logMessage(logger, curThreadName, "success: entered for trxnId = %d", trxnId);
		//successfulTransactions.incrementAndGet();
		//Util.logMessage(logger, curThreadName, "success: exiting with updated succesfulTrxns = %d", successfulTransactions.get());

	}

	/**
     * Release any resources held by this emitter.
     */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
