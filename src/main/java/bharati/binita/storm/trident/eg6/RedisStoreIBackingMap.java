package bharati.binita.storm.trident.eg6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;
import bharati.binita.storm.trident.util.CommonUtil;

import redis.clients.jedis.Jedis;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

/**
 * 
 * @author binita.bharati@gmail.com
 * Inbuilt different implementations of IBackingMap are there. E.g : CachedMap<T>, CassandraState<T>, MemoryBackingMap etc.
 * Based on your underlying storage, you need to write appropriate implementations of your own (if inbuilt do not suffice).
 * 
 * What is the relation between multiGet and multiPut ?
 * multiGet is meant to filter out keys that are further passed on to multiput.
 * ie , all the keys for which value is null (as decided by multiGet), will get passed on to multiPut.
 * Check storm.trident.state.map.TransactionalMap:multiUpdate
 *
 */
public class RedisStoreIBackingMap implements IBackingMap<TransactionalValue<Long>>{
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreIBackingMap.class);

	private RedisOperations redisOperations;
		
	private String failedWord;
	
	public RedisStoreIBackingMap(String host, String port, String failedWord)
	{
		this.redisOperations = new RedisOperations(host, port);
		
		this.failedWord = failedWord;
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "RedisStoreIBackingMap: failedWord = %s", failedWord);


	}
	
   
	@Override
	public List<TransactionalValue<Long>> multiGet(
			List<List<Object>> keys) {
		// TODO Auto-generated method stub
		List<TransactionalValue<Long>> retList = new ArrayList<>();
		for(List<Object> eachKeyList : keys)
		{
			String word = (String)eachKeyList.get(0);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiGet: word = %s", word);

			List<String> redisVal = redisOperations.lrange(word); 
			
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiGet: redisVal = %s", redisVal);

			if(redisVal != null && redisVal.size() > 0)
			{
				Long txId = Long.parseLong(redisVal.get(0));
				Long wordCount = Long.parseLong(redisVal.get(1));
				
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiGet: txId = %d and count = %d for word = %s", txId, wordCount, word);
				TransactionalValue<Long> tv = new TransactionalValue<Long>(txId, wordCount);
				retList.add(tv);

			}
			else
			{
				retList.add(null);
			}
		}
		return retList;
	}

	@Override
	public void multiPut(List<List<Object>> keys,
			List<TransactionalValue<Long>> vals) {
		// TODO Auto-generated method stub
		try {
			throw new Exception("multiPut: throwing intentional Exception - check code flow");
			/**
			 * 	2014-12-30 11:28:09 STDIO [ERROR] at bharati.binita.storm.trident.eg6.RedisStoreIBackingMap.multiPut(RedisStoreIBackingMap.java:86)
				2014-12-30 11:28:09 STDIO [ERROR] at bharati.binita.storm.trident.eg6.RedisStoreStateFactory$2.multiPut(RedisStoreStateFactory.java:126)
				2014-12-30 11:28:09 STDIO [ERROR] at bharati.binita.storm.trident.eg6.RedisStoreStateFactory$3.multiPut(RedisStoreStateFactory.java:148)
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.state.map.CachedBatchReadsMap.multiPut(CachedBatchReadsMap.java:66)
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.state.map.TransactionalMap.multiUpdate(TransactionalMap.java:84)
				2014-12-30 11:28:09 b.s.disruptor [INFO] disruptor: consume-batch-when-available entered
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.state.map.MapCombinerAggStateUpdater.updateState(MapCombinerAggStateUpdater.java:73)
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.state.map.MapCombinerAggStateUpdater.updateState(MapCombinerAggStateUpdater.java:39)
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.planner.processor.PartitionPersistProcessor.finishBatch(PartitionPersistProcessor.java:113)
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.planner.SubtopologyBolt.finishBatch(SubtopologyBolt.java:159)
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.topology.TridentBoltExecutor.finishBatch(TridentBoltExecutor.java:264)
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.topology.TridentBoltExecutor.checkFinish(TridentBoltExecutor.java:299)
				2014-12-30 11:28:09 b.s.disruptor [INFO] disruptor: consume-batch-when-available entered
				2014-12-30 11:28:09 STDIO [ERROR] at storm.trident.topology.TridentBoltExecutor.execute(TridentBoltExecutor.java:392)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.daemon.executor$fn__5641$tuple_action_fn__5643.invoke(executor.clj:631)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.daemon.executor$mk_task_receiver$fn__5564.invoke(executor.clj:399)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.disruptor$clojure_handler$reify__43.onEvent(disruptor.clj:58)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:125)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:99)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:81)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.daemon.executor$fn__5641$fn__5653$fn__5700.invoke(executor.clj:746)
				2014-12-30 11:28:09 STDIO [ERROR] at backtype.storm.util$async_loop$fn__457.invoke(util.clj:431)
				2014-12-30 11:28:09 STDIO [ERROR] at clojure.lang.AFn.run(AFn.java:24)
				2014-12-30 11:28:09 STDIO [ERROR] at java.lang.Thread.run(Thread.java:744)

			 */
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: entered with keys = %s, vals = %s", keys, vals);
		
		int curIndex = 0;
		for(List<Object> eachKeyList : keys)
		{
			String word = (String)eachKeyList.get(0);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: key = %s", word);
			
			TransactionalValue<Long> curKeyVal = vals.get(curIndex);
			Long currentTxId = curKeyVal.getTxid();//Get current trxn id from the input argument only - Check the implementation of TransactionalMap:multiPut.
			
			if(word.equals(failedWord))
			{
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: Throwing FailedException for word = %s, trxId = %d", word, currentTxId);

				throw new FailedException("FailedException thrown for word = "+word +" in trxId = "+currentTxId);
			}
					
			
			/* Transactional State Management behaviour :
			 * Check if word(key) is already present in Redis.
			 * If present
			 *      Check the txId and the wordCount value stored against the key.
			 * 		If the current txId is higher than the stored txId, 
			 * 			Then update the wordCount.
			 *  		Else, ignore the data in the current trxn
			 * If not present
			 *         Add the word as key and (trxId , wordCount) as the value.
			 * 
			 * */
			
			List<String> redisVal = redisOperations.lrange(word);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: redisVal = %s", redisVal);
			
			Long redisTxId = -1L;
			Long redisWordCount = 0L;

			boolean redisKeyExists = false;
			if(redisVal != null && redisVal.size() > 0)
			{
				redisTxId = Long.parseLong(redisVal.get(0));
				redisWordCount = Long.parseLong(redisVal.get(1));
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: redisTxId = %d, redisWordCount = %d", redisTxId,redisWordCount);
				redisKeyExists = true;
				

			}
			else
			{
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: no entry in Redis yet for word = %s",word);

			}

			
			
			Long wordCount = curKeyVal.getVal();
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: currentTxId = %d, wordCount = %d", currentTxId, wordCount);
			
			
			Long latestWordCount = redisWordCount + wordCount;
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: latestWordCount = %d", latestWordCount);
			
				
			if(!redisKeyExists) //first time insert happening for a key
			{
				redisOperations.rpush(word, currentTxId+"", latestWordCount+"");
			}
			else//first pop out the old values and insert new values.
			{
				redisOperations.lpop(word, 2);
				redisOperations.rpush(word, currentTxId+"", latestWordCount+"");
				
			}
						
			curIndex++;	

		}

		
	}
	
}
