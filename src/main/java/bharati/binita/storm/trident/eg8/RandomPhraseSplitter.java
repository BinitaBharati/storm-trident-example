package bharati.binita.storm.trident.eg8;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.storm.trident.util.CommonUtil;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author binita.bharati@gmail.com
 * Tokenize the incoming phrase using a Lucene filter.
 *
 */
public class RandomPhraseSplitter extends BaseFunction{
	
	private static Logger logger = LoggerFactory.getLogger(RandomPhraseSplitter.class);

	@Override
	public void execute(TridentTuple tuple,
			TridentCollector collector) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();
		String randomPhrase = tuple.getStringByField("randomPhrase");
		
		CommonUtil.logMessage(logger, currentThreadName, "execute: entered with tuple = %s", randomPhrase);
		
		//Eliminate stop words and punctuation like comma , double quotes - use Lucene
		List<String> filteredFailedPhrase = Util.filterPhrase2(randomPhrase);
		 
		CommonUtil.logMessage(logger, currentThreadName, "execute: filteredFailedPhraseList = %s", filteredFailedPhrase);		

		
		for(String eachWord : filteredFailedPhrase)
		{
			
			List<Object> tupleList = new ArrayList<>();
			tupleList.add(eachWord);
			CommonUtil.logMessage(logger, currentThreadName, "execute: emitting tuple with word = %s", eachWord);

			collector.emit(tupleList);
			
		}
		
	}

}
