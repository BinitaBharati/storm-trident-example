package bharati.binita.storm.trident.eg3;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
public class RedisStoreIBackingMap implements IBackingMap<TransactionalValue<Long>>{
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreIBackingMap.class);

	private Jedis jedis;
	
	public RedisStoreIBackingMap(String host, String port)
	{
		connectToRedis(host, port);
	}
	
	private void connectToRedis(String host, String port)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "connectToRedis: entered with host = %s, port = %s", host, port);
		//jedis = new Jedis(host, Integer.valueOf(port));
		jedis = new Jedis(host);
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

			List<String> redisVal = lrange(word);
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
			
			List<String> redisVal = lrange(word);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: redisVal = %s", redisVal);
			
			Long redisTxId = -1L;
			Long redisWordCount = 0L;
			Long currentTxId = -1L;	//Get current trxn id from the input argument only - Check the implementation of TransactionalMap:multiPut.

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

			
			TransactionalValue<Long> curKeyVal = vals.get(curIndex);
			currentTxId = curKeyVal.getTxid();
			Long wordCount = curKeyVal.getVal();
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: currentTxId = %d, wordCount = %d", currentTxId, wordCount);
			
			if(currentTxId != redisTxId && currentTxId > redisTxId)//Latest trxn id numbers will always be higher than previous ones.
			{
				Long latestWordCount = redisWordCount + wordCount;
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: latestWordCount = %d", latestWordCount);
				
				if(!redisKeyExists) //first time insert happening for a key
				{
					rpush(word, currentTxId+"", latestWordCount+"");
				}
				else//first pop out the old values and insert new values.
				{
					lpop(word, 2);
					rpush(word, currentTxId+"", latestWordCount+"");
					
				}
					
			}
			
			curIndex++;	

		}

		
	}
	//rpush data structure can be used both like a queue/stack (via lpop/rpop)
	private void rpush(String word, String txId, String wordCount)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "rpush: entered with word = %s, txId = %s, wordCount = %s", word, txId, wordCount);
		jedis.rpush(word, txId );
		jedis.rpush(word, wordCount );

	}
	
	//rpop is a LIFO operation
	private void rpop(String word, int popCount)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "rpop: entered with word = %s", word);
		for(int i = 0 ; i < popCount ; i++)
		{
			jedis.rpop(word);
		}
		

	}
	
   //lpop is a FIFO operation
	private void lpop(String word, int popCount)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "lpop: entered with word = %s", word);
		for(int i = 0 ; i < popCount ; i++)
		{
			jedis.lpop(word);
		}

	}
	
	private List<String> lrange(String word)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "lrange: entered with word = %s", word);
		return jedis.lrange(word, 1, -2);

	}
	

	
}
