package bharati.binita.storm.trident.eg8;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.IMetricsContext;

import bharati.binita.storm.trident.eg6.Util;
import bharati.binita.storm.trident.util.CommonUtil;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.OpaqueValue;
import storm.trident.state.map.IBackingMap;

import org.apache.commons.io.IOUtils;

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

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
		Integer phraseCount = Integer.parseInt((String)conf.get("phraseCount"));

		
		CommonUtil.logMessage(logger, curThreadName, "makeState: redisServerIP = %s, redisServerPort = %s ", 
				redisServerIP, redisServerPort);
		
		//Determine failedWord - The word the presence of which should fail a trxn. - start
				String[] sentences = null;
				try {
					sentences = (String[]) IOUtils.readLines(
					    ClassLoader.getSystemClassLoader().getResourceAsStream("500_sentences_en.txt")).toArray(new String[0]);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				Random rand = new Random();
				String failedPhrase = sentences[rand.nextInt(phraseCount)];
				CommonUtil.logMessage(logger, curThreadName, "makeState: failedPhrase = %s", 
						failedPhrase);
				//Eliminate stop words and punctuation like comma , double quotes - use Lucene
				List<String> filteredFailedPhrase = Util.filterPhrase2(failedPhrase);		 
				CommonUtil.logMessage(logger, curThreadName, "makeState: filteredFailedPhrase = %s", filteredFailedPhrase);		
				//String failOnWord = filteredFailedPhrase.get(rand.nextInt(filteredFailedPhrase.size()));
				String failOnWord = "outclass";
				CommonUtil.logMessage(logger, curThreadName, "makeState: failOnWord = %s", 
						failOnWord);
				//Determine failedWord - The word the presence of which should fail a trxn. - end
		
		IBackingMap<OpaqueValue<Long>> backMap = new RedisStoreIBackingMap(redisServerIP, redisServerPort, failOnWord);
		
		IBackingMap<OpaqueValue> test = generic(generic(backMap, Long.class));
		
		return new RedisStoreState(test);
	}

	public static <T> IBackingMap<OpaqueValue<T>> coerce(final IBackingMap<OpaqueValue<?>> whatever, final Class<T> clazz) {
		return new IBackingMap<OpaqueValue<T>>() {
			@Override
			public List<OpaqueValue<T>> multiGet(List<List<Object>> paramList) {
				List<OpaqueValue<T>> result = new ArrayList<>();
				for (OpaqueValue<?> each: whatever.multiGet(paramList)) {
					result.add(new OpaqueValue<T>(each.getCurrTxid(), clazz.cast(each.getCurr()), clazz.cast(each.getPrev())));
				}
				return result;
			}
			@Override
			public void multiPut(List<List<Object>> paramList, List<OpaqueValue<T>> paramList1) {
				List<OpaqueValue<?>> arg2 = new ArrayList<>();
				for (OpaqueValue<T> each: paramList1) {
					arg2.add(new OpaqueValue<T>(each.getCurrTxid(), clazz.cast(each.getCurr()), clazz.cast(each.getPrev())));
				}
				whatever.multiPut(paramList, arg2);
			}
		};
	}

	public static <T> IBackingMap<OpaqueValue<?>> generic(final IBackingMap<OpaqueValue<T>> whatever, final Class<T> clazz) {
		return new IBackingMap<OpaqueValue<?>>() {
			@Override
			public List<OpaqueValue<?>> multiGet(List<List<Object>> paramList) {
				List<OpaqueValue<?>> result = new ArrayList<>();
				result.addAll(whatever.multiGet(paramList));
				/*for (OpaqueValue<?> each: whatever.multiGet(paramList)) {
					result.add(new OpaqueValue<T>(each.getTxid(), clazz.cast(each.getVal())));
				}*/
				return result;
			}
			@Override
			public void multiPut(List<List<Object>> paramList, List<OpaqueValue<?>> paramList1) {
				List<OpaqueValue<T>> arg2 = new ArrayList<>();
				for (OpaqueValue<?> each: paramList1) {
					arg2.add(new OpaqueValue<T>(each.getCurrTxid(), clazz.cast(each.getCurr()), clazz.cast(each.getPrev())));
				}
				whatever.multiPut(paramList, arg2);
			}
		};
	}

	public static IBackingMap<OpaqueValue> generic(final IBackingMap<OpaqueValue<?>> whatever) {
		return new IBackingMap<OpaqueValue>() {
			@Override
			public List<OpaqueValue> multiGet(List<List<Object>> paramList) {
				List<OpaqueValue> result = new ArrayList<>();
				/*for (OpaqueValue each: whatever.multiGet(paramList)) {
					result.add(each);
				}*/
				result.addAll(whatever.multiGet(paramList));
				return result;
			}
			@Override
			public void multiPut(List<List<Object>> paramList, List<OpaqueValue> paramList1) {
				List<OpaqueValue<?>> arg2 = new ArrayList<>();
				for (OpaqueValue<?> each: paramList1) {
					arg2.add(each);
				}
				whatever.multiPut(paramList, arg2);
			}
		};
	}

}
