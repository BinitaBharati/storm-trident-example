package bharati.binita.storm.trident.eg5;

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
 *
 *
 * Do not have static fields in your Storm/Trident components:
 * https://groups.google.com/forum/#!topic/storm-user/Dlpg3G8rx7w
 *
 */

public class RandomPhraseEmitter implements Emitter<String>, Serializable{//Object is the type of the tuple that will be emitted
	
	private static Logger logger = LoggerFactory.getLogger(RandomPhraseEmitter.class);
	private static Random rand = new Random();
	private static int latestIndex = 0;//wrong to have this in a distributed system - use some storage layer to achieve the same
		
	private String[] sentences;
	
	private RedisOperations redisOperations;

		
	public RandomPhraseEmitter(String redisServerIP, String redisServerPort)
	{			
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
     * was created by the Coordinator in the initializeTranaction method. This method must always emit
     * the same batch of tuples across all tasks for the same transaction id - Transactional Spout feature.
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
		long trxnId = trxnAttempt.getTransactionId();
		CommonUtil.logMessage(logger, curThreadName, "emitBatch: entered with latestIndex = %d",  latestIndex);


		CommonUtil.logMessage(logger, curThreadName, "emitBatch: entered with trxnId = %d",  trxnId);
		/**
		 *  2014-11-28 21:07:16 b.s.util [ERROR] Async loop died!
               java.lang.RuntimeException: java.lang.ArrayIndexOutOfBoundsException: 498
            2014-11-28 21:07:16 b.s.util [INFO] Halting process: ("Worker died")
            014-11-28 21:13:43 b.s.d.worker [INFO] Launching worker for   trident-eg5-2-1417188174 on 223733d0-502e-4bfd-a074-ca39e7d5c130:6703 with  id e4c3658f-881b-493b-8304-65bc01ddf18a and conf {"dev.zookeeper.path"  "/tmp/dev-storm-zookeeper".......................

		 */
		if(latestIndex < 498)//To stop java.lang.ArrayIndexOutOfBoundsException , and to prevent worker from dying and restarting.
		{
			String randomPhrase = sentences[latestIndex];
			latestIndex++;
		    
			CommonUtil.logMessage(logger, curThreadName, "emitBatch: randomPhrase = %s",  randomPhrase);
					
			List<Object> randomPhraseList = new ArrayList<>();
			randomPhraseList.add(randomPhrase);
			
			collector.emit(randomPhraseList);
		}
		
			
	}

	 /**
     * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
     */

	@Override
	public void success(TransactionAttempt trxnAttempt) {
		// TODO Auto-generated method stub
		long trxnId = trxnAttempt.getTransactionId();
		String curThreadName = Thread.currentThread().getName();
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
