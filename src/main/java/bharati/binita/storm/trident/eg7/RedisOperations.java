package bharati.binita.storm.trident.eg7;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.main;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import bharati.binita.storm.trident.util.CommonUtil;

/**
 * 
 * @author binita.bharati@gmail.com
 * 
 */

public class RedisOperations {
	
	private static Logger logger = LoggerFactory.getLogger(RedisOperations.class);
	
	private JedisPool jedisPool;
	
	public RedisOperations(String redisServerIP, String redisServerPort)
	{
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        this.jedisPool = new JedisPool(poolConfig, redisServerIP, Integer.parseInt(redisServerPort));
	}

	
	//rpush data structure can be used both like a queue/stack (via lpop/rpop)
	public void rpush(String key, String... vals)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "rpush: entered with key = %s, vals = %s", key, Arrays.asList(vals));
		
		Jedis jedis = jedisPool.getResource();
		
		jedis.rpush(key, vals);
		
		jedisPool.returnResourceObject(jedis);		

	}
		
	//rpop is a LIFO operation
	public void rpop(String key, int popCount)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "rpop: entered with key = %s", key);
		Jedis jedis = jedisPool.getResource();
		for(int i = 0 ; i < popCount ; i++)
		{
			jedis.rpop(key);
		}
		jedisPool.returnResourceObject(jedis);

	}
		
   //lpop is a FIFO operation
	public void lpop(String key, int popCount)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "lpop: entered with key = %s", key);
		Jedis jedis = jedisPool.getResource();
		for(int i = 0 ; i < popCount ; i++)
		{
			jedis.lpop(key);
		}
		jedisPool.returnResourceObject(jedis);

	}
		
	public List<String> lrange(String key, int startIdx, int endIdx)
	{
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "lrange: entered with key = %s", key);
		Jedis jedis = jedisPool.getResource();
		
		List<String> vals = jedis.lrange(key, startIdx, endIdx);
		jedisPool.returnResourceObject(jedis);

		return vals;
	}
	
	public static void main(String[] args) {
		RedisOperations ro = new RedisOperations("192.168.1.2", "6379");
		
		ro.lrange("test", 0 , 1);
	}

}
