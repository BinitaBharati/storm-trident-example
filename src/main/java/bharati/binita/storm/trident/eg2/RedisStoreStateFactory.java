package bharati.binita.storm.trident.eg2;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.IMetricsContext;
import bharati.binita.storm.trident.util.CommonUtil;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

/**
 * 
 * @author binita.bharati@gmail.com
 * A StateFactory that should return a instance of the underlying State.
 *
 */

public class RedisStoreStateFactory implements StateFactory {
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreStateFactory.class);


	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		// TODO Auto-generated method stub
		String curThreadName = Thread.currentThread().getName();

		CommonUtil.logMessage(logger, curThreadName, "makeState: entered partitionIndex = %d, numPartitions = %d ", 
				partitionIndex, numPartitions);
		
		String redisServerIP = (String)conf.get("redisServerIP");
		String redisServerPort = (String)conf.get("redisServerPort");
		
		CommonUtil.logMessage(logger, curThreadName, "makeState: redisServerIP = %s, redisServerPort = %s ", 
				redisServerIP, redisServerPort);
		
		return new RedisStoreState(redisServerIP, redisServerPort);
	}

}
