package bharati.binita.redis.client.example;

import java.util.List;

import bharati.binita.storm.trident.util.CommonUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import clojure.main;

/**
 * http://redis.io/commands
 */


public class JedisClientTest {
	
	private JedisPool jedisPool;
	
	public JedisClientTest()
	{
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        this.jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6379);
		
	}
	
	public static void main(String[] args) {
		
		JedisClientTest temp = new JedisClientTest();
		temp.rpush("b", "111", "-1");
		System.out.println(temp.lrange("b"));
		
	}
	
	private List<String> lrange(String key)
	{
		Jedis jedis = jedisPool.getResource();
		
		List<String> vals = jedis.lrange(key, 0, 1);
		jedisPool.returnResourceObject(jedis);

		return vals;
	}
	
	private  void rpush(String key, String... vals)
	{
		Jedis jedis = jedisPool.getResource();
		
		jedis.rpush(key, vals);
		
		jedisPool.returnResourceObject(jedis);	
	}

}
