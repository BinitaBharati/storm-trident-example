package bharati.binita.storm.trident.eg3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.IMetricsContext;
import bharati.binita.storm.trident.util.CommonUtil;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

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
		
		IBackingMap<TransactionalValue<Long>> backMap = new RedisStoreIBackingMap(redisServerIP, redisServerPort);
		
		IBackingMap<TransactionalValue> test = generic(generic(backMap, Long.class));
		
		return new RedisStoreState(test);
	}

	public static <T> IBackingMap<TransactionalValue<T>> coerce(final IBackingMap<TransactionalValue<?>> whatever, final Class<T> clazz) {
		return new IBackingMap<TransactionalValue<T>>() {
			@Override
			public List<TransactionalValue<T>> multiGet(List<List<Object>> paramList) {
				List<TransactionalValue<T>> result = new ArrayList<>();
				for (TransactionalValue<?> each: whatever.multiGet(paramList)) {
					result.add(new TransactionalValue<T>(each.getTxid(), clazz.cast(each.getVal())));
				}
				return result;
			}
			@Override
			public void multiPut(List<List<Object>> paramList, List<TransactionalValue<T>> paramList1) {
				List<TransactionalValue<?>> arg2 = new ArrayList<>();
				for (TransactionalValue<T> each: paramList1) {
					arg2.add(new TransactionalValue<T>(each.getTxid(), each.getVal()));
				}
				whatever.multiPut(paramList, arg2);
			}
		};
	}

	public static <T> IBackingMap<TransactionalValue<?>> generic(final IBackingMap<TransactionalValue<T>> whatever, final Class<T> clazz) {
		return new IBackingMap<TransactionalValue<?>>() {
			@Override
			public List<TransactionalValue<?>> multiGet(List<List<Object>> paramList) {
				List<TransactionalValue<?>> result = new ArrayList<>();
				result.addAll(whatever.multiGet(paramList));
				/*for (TransactionalValue<?> each: whatever.multiGet(paramList)) {
					result.add(new TransactionalValue<T>(each.getTxid(), clazz.cast(each.getVal())));
				}*/
				return result;
			}
			@Override
			public void multiPut(List<List<Object>> paramList, List<TransactionalValue<?>> paramList1) {
				List<TransactionalValue<T>> arg2 = new ArrayList<>();
				for (TransactionalValue<?> each: paramList1) {
					arg2.add(new TransactionalValue<T>(each.getTxid(), clazz.cast(each.getVal())));
				}
				whatever.multiPut(paramList, arg2);
			}
		};
	}

	public static IBackingMap<TransactionalValue> generic(final IBackingMap<TransactionalValue<?>> whatever) {
		return new IBackingMap<TransactionalValue>() {
			@Override
			public List<TransactionalValue> multiGet(List<List<Object>> paramList) {
				List<TransactionalValue> result = new ArrayList<>();
				/*for (TransactionalValue each: whatever.multiGet(paramList)) {
					result.add(each);
				}*/
				result.addAll(whatever.multiGet(paramList));
				return result;
			}
			@Override
			public void multiPut(List<List<Object>> paramList, List<TransactionalValue> paramList1) {
				List<TransactionalValue<?>> arg2 = new ArrayList<>();
				for (TransactionalValue<?> each: paramList1) {
					arg2.add(each);
				}
				whatever.multiPut(paramList, arg2);
			}
		};
	}

}
