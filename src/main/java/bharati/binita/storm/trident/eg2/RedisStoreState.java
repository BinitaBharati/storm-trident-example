package bharati.binita.storm.trident.eg2;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.storm.trident.util.CommonUtil;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;

/**
 * 
 * @author binita.bharati@gmail.com
 * https://storm.apache.org/documentation/Trident-state
 * 
 * Transactional state management
 * Same trxn id always carries same set of tuples. 
 * So, even when a purely transactional batch is replayed, same set of tuples should be present for that trxn as before.
 *
 */

public class RedisStoreState implements State{
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreState.class);

	private Jedis jedis;
	
	private void connectToRedis(String host, String port)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "connectToRedis: entered with host = %s, port = %s", host, port);
		//jedis = new Jedis(host, Integer.valueOf(port));
		jedis = new Jedis(host);
	}
	
	public RedisStoreState(String host, String port)
	{
		connectToRedis(host, port);
	}

	@Override
	/**
	 * Invoked before committing to the underlying store.
	 * Not used.
	 */
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();
		
		CommonUtil.logMessage(logger, currentThreadName, "beginCommit: entered with txid = %d", txid);
		
	}

	@Override
	/**
	 * Invoked while committing to the underlying store.
	 * Not used.
	 */
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();

		CommonUtil.logMessage(logger, currentThreadName, "commit: entered with txid = %d", txid);

		
	}
	
	/**
	 * 
	 * @param wordKey
	 * @param wordCount
	 * Method can be directly called from StateUpdater. The StateUpdater wil have access to a instance of this class.
	 */
	public void set(String wordKey,String wordCount)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "set: entered with wordKey = %s, wordCount = %s", wordKey, wordCount);

		jedis.set(wordKey, wordCount);
	}
	
	/**
	 * 
	 * @param wordKey
	 * Method can be directly called from StateUpdater. The StateUpdater wil have access to a instance of this class.
	 */
	public String get(String wordKey)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "get: entered with wordKey = %s", wordKey);
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "get: jedis = %s", jedis);

		return jedis.get(wordKey);
	}

}
