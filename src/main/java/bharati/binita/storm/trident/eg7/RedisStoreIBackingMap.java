package bharati.binita.storm.trident.eg7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;
import bharati.binita.storm.trident.util.CommonUtil;

import redis.clients.jedis.Jedis;
import storm.trident.state.OpaqueValue;
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
public class RedisStoreIBackingMap implements IBackingMap<OpaqueValue<Long>>{
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreIBackingMap.class);

	private RedisOperations redisOperations;
			
	public RedisStoreIBackingMap(String host, String port)
	{
		this.redisOperations = new RedisOperations(host, port);
		

	}
	
   
	@Override
	public List<OpaqueValue<Long>> multiGet(
			List<List<Object>> keys) {
		// TODO Auto-generated method stub
		List<OpaqueValue<Long>> retList = new ArrayList<>();
		for(List<Object> eachKeyList : keys)
		{
			String word = (String)eachKeyList.get(0);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiGet: word = %s", word);

			List<String> redisVal = redisOperations.lrange(word, 0 , 2); 
			
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiGet: redisVal = %s", redisVal);

			if(redisVal != null && redisVal.size() > 0)
			{
				Long txId = Long.parseLong(redisVal.get(0));
				Long currentWc = Long.parseLong(redisVal.get(1));
				Long prevWc = Long.parseLong(redisVal.get(2));

				
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiGet: txId = %d, prevWc = %d, currentWc = %d for word = %s", txId, prevWc, currentWc, word);
				OpaqueValue<Long> tv = new OpaqueValue<Long>(txId, currentWc, prevWc);
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
			List<OpaqueValue<Long>> vals) {
		// TODO Auto-generated method stub
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: entered with keys = %s, vals = %s", keys, vals);
		
		//checking code flow - start
		/*try{
			throw new Exception("check flow");
		}
		catch(Exception ex)
		{
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: check flow exception caught");

			ex.printStackTrace();
		}*/
		
		//checking code flow - end
		
		int curIndex = 0;
		for(List<Object> eachKeyList : keys)
		{
			String word = (String)eachKeyList.get(0);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: key = %s", word);
			
			OpaqueValue<Long> curKeyVal = vals.get(curIndex);
			Long currentTxId = curKeyVal.getCurrTxid();//Get current trxn id from the input argument only - Check the implementation of TransactionalMap:multiPut.
			/**
			 * Opaque state management :
			 * The behaviour of Opaque state as depicted in https://storm.incubator.apache.org/documentation/Trident-state
			 * is already being handled at storm.trident.state.map.OpaqueMap:multiUpdate API.
			 * So, you don't need to do all the housekeeping work, its already handled.
			 * All that you need to do is check if key exists in Redis, If yes, pop it out and push.
			 * Else, just push.
			 * 
			 */
			Long currentWordCount = curKeyVal.getCurr();
			Long curentPrevWordCount = 0L;//init to 0 as the value comes as null for first time.
			Object curentPrevWordCountObj = curKeyVal.getPrev();
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: curentPrevWordCountObj = %s", curentPrevWordCountObj);

			if(curentPrevWordCountObj != null)
			{
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: if1");

				curentPrevWordCount = (Long)curentPrevWordCountObj;
			}
			
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: key = %s, currentTxId = %d, currentWordCount = %d, curentPrevWordCount = %d" , 
					word, currentTxId, currentWordCount, curentPrevWordCount);

			
			List<String> redisVal = redisOperations.lrange(word, 0 , 2);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: redisVal = %s", redisVal);
			
			if(redisVal != null && redisVal.size() > 0)
			{
				redisOperations.lpop(word, 3);
				redisOperations.rpush(word, currentTxId+"", currentWordCount+"", curentPrevWordCount + "");
				

			}
			else
			{
				CommonUtil.logMessage(logger, Thread.currentThread().getName(), "multiPut: no entry in Redis yet for word = %s",word);
				redisOperations.rpush(word, currentTxId+"", currentWordCount+"", curentPrevWordCount + "");


			}
						
			curIndex++;	

		}

		
	}
	
}
