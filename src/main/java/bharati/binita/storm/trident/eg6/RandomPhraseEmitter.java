package bharati.binita.storm.trident.eg6;

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
	private static int latestIndex = 0;//wrong to have this in a distributd system - use some storage layer to achieve teh same
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
     * was created by the Coordinator in the initializeTranaction method. This method must always emit
     * the same batch of tuples across all tasks for the same transaction id - Transactional Spout feature..
     * 
     * emitBatch will keep getting called repeatedly. Each invocation to emitBatch will be done in a new trxn.
     * emitBatch is internally invoked by Storm repeatedly, like a infinite while loop.Each iteration of the
     * loop is a trxn.Trxn Ids are auto-generated.
     * 
     * In case of batch replay, same failed phrase will keep getting emitted over and over for the same trxnId.
     * 
     */

	@Override
	public void emitBatch(TransactionAttempt trxnAttempt, String coordinatorMeta,
			TridentCollector collector) {
		// TODO Auto-generated method stub
		
		String curThreadName = Thread.currentThread().getName();
		/* During batch replay scenario, failed trxId will be repeated.
		 * i.e you will see the same trxId more than once. */
		long trxnId = trxnAttempt.getTransactionId();
		CommonUtil.logMessage(logger, curThreadName, "emitBatch: entered with latestIndex = %d",  latestIndex);


		CommonUtil.logMessage(logger, curThreadName, "emitBatch: entered with trxnId = %d",  trxnId);
				
		
		/**
		 * Store the trxId mapping to data. Used for replay during batch (trxn) failure scenario.  - start
		 */
		List<String> redisData = redisOperations.lrange(trxnId+"");
		CommonUtil.logMessage(logger, curThreadName, "emitBatch: redisData = %s",  redisData);
		
		String randomPhrase = null;
		
		if(redisData != null && redisData.size() > 0)
		{
			//Implies this trxn is replayed!!
			CommonUtil.logMessage(logger, curThreadName, "emitBatch: trxn = %s is replayed !!",  trxnId);
			randomPhrase = redisData.get(0);
			
		}
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
			redisOperations.rpush(trxnId+"", Util.filterPhrase(randomPhrase));
			
		}
		/**
		 * Store the trxId mapping to data. Used for replay during batch (trxn) failure scenario.  - end
		 */
		
	    
		CommonUtil.logMessage(logger, curThreadName, "emitBatch: randomPhrase = %s",  randomPhrase);
				
		List<Object> randomPhraseList = new ArrayList<>();
		randomPhraseList.add(randomPhrase);
		
		collector.emit(randomPhraseList);
			
	}

	 /**
     * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
     * In case of Transactional State Management, 
     * success will NOT be called when a batch is explicitly failed - by throwing FailedException.
     */

	@Override
	public void success(TransactionAttempt trxnAttempt) {
		// TODO Auto-generated method stub
		/**
		 * 	at bharati.binita.storm.trident.eg6.RandomPhraseEmitter.success(RandomPhraseEmitter.java:150)
			at storm.trident.spout.TridentSpoutExecutor.execute(TridentSpoutExecutor.java:79)
			at storm.trident.topology.TridentBoltExecutor.execute(TridentBoltExecutor.java:333)
			at backtype.storm.daemon.executor$fn__5641$tuple_action_fn__5643.invoke(executor.clj:631)
			at backtype.storm.daemon.executor$mk_task_receiver$fn__5564.invoke(executor.clj:399)
			at backtype.storm.disruptor$clojure_handler$reify__745.onEvent(disruptor.clj:58)
			at backtype.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:125)
			at backtype.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:99)
			at backtype.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:80)
		 */
		try {
			throw new Exception("success: throwing intentional Exception - check code flow");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
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
