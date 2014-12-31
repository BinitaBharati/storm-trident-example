package bharati.binita.storm.trident.eg5;

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
 *
 */
public class RedisStoreIBackingMap implements IBackingMap<TransactionalValue<Long>>{
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreIBackingMap.class);

	private RedisOperations redisOperations;
		
	private String failedWord;
	
	public RedisStoreIBackingMap(String host, String port)
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

			/**
			 * The below line is commented intentionally. Why ? To illustrate batch replay use case.
			 * I want the entire original phrase to be passed into multiPut, and not only the parts
			 * that failed to be stored into Redis due to FailedException.
			 */
			//List<String> redisVal = redisOperations.lrange(word); 
			
			List<String> redisVal = new ArrayList<>();
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
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: entered with keys = %s, vals = %s", keys, vals);
		
		int curIndex = 0;
		for(List<Object> eachKeyList : keys)
		{
			String word = (String)eachKeyList.get(0);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: key = %s", word);
			
			TransactionalValue<Long> curKeyVal = vals.get(curIndex);
			Long currentTxId = curKeyVal.getTxid();//Get current trxn id from the input argument only - Check the implementation of TransactionalMap:multiPut.
			
			
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
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: count = %s for word = %s", redisVal, word);
			
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
			
			if(currentTxId != redisTxId && currentTxId > redisTxId)//Latest trxn id numbers will always be higher than previous ones.
			{
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
				
					
			}
			
			curIndex++;	

		}

		
	}
	
}
