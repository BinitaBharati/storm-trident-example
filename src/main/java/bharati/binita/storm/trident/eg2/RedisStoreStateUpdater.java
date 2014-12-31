package bharati.binita.storm.trident.eg2;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.storm.trident.util.CommonUtil;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author binita.bharati@gmail.com
 * The StateUpdater should define the updateState method.
 * The updateState method will have access to the State instance.
 * Any custom method written on the State instance can then be invoked.
 *
 */

public class RedisStoreStateUpdater implements StateUpdater<RedisStoreState>{
	
	private static final long serialVersionUID = 1;
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreState.class);

	@Override
	public void prepare(Map paramMap,
			TridentOperationContext paramTridentOperationContext) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateState(RedisStoreState redisStoreState, List<TridentTuple> tupleList,
			TridentCollector collector) {
		// TODO Auto-generated method stub
		CommonUtil.logMessage(logger, Thread.currentThread().getName(), "updateState: entered with tupleList = %s", tupleList);
		for (TridentTuple eachTuple : tupleList)
		{
			String word = eachTuple.getString(0);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "updateState: word = %s", word);
			
			String wordCount = redisStoreState.get(word);
			CommonUtil.logMessage(logger, Thread.currentThread().getName(), "updateState: word = %s, wordCount = %s", word, wordCount);
			
			if(wordCount != null)
			{
				Integer wordCountIntNew = Integer.parseInt(wordCount) + 1;
				redisStoreState.set(word, wordCountIntNew + "");

			}
			else//seen this word for the first time
			{
				redisStoreState.set(word, "1");
			}



		}
		
		
	}



}
