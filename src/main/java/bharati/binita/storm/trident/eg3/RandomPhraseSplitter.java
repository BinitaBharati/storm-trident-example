package bharati.binita.storm.trident.eg3;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.storm.trident.util.CommonUtil;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class RandomPhraseSplitter extends BaseFunction{
	
	private static Logger logger = LoggerFactory.getLogger(RandomPhraseSplitter.class);

	@Override
	public void execute(TridentTuple tuple,
			TridentCollector collector) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();
		String randomPhrase = tuple.getStringByField("randomPhrase");
		
		CommonUtil.logMessage(logger, currentThreadName, "execute: entered with tuple = %s", randomPhrase);
		
		StringTokenizer st = new StringTokenizer(randomPhrase);
		
		while(st.hasMoreTokens())
		{
			String token = st.nextToken();
			
			List<Object> tupleList = new ArrayList<>();
			tupleList.add(token);
			CommonUtil.logMessage(logger, currentThreadName, "execute: emitting tuple with word = %s", token);

			collector.emit(tupleList);
			
		}
		
	}

}
