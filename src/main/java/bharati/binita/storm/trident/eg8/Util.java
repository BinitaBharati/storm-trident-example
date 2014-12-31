package bharati.binita.storm.trident.eg8;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.storm.trident.util.CommonUtil;


public class Util {
	
	private static Logger logger = LoggerFactory.getLogger(Util.class);

	
	//Eliminate stop words and punctuation like comma , double quotes - use Lucene
	public static String filterPhrase(String pharse)
	{	
		String currentThreadName = Thread.currentThread().getName();

		CommonUtil.logMessage(logger, currentThreadName, "filterPhrase: entering with %s", pharse);		
		List<String> filteredPhraseList = new ArrayList<>();
		 TokenStream ts = null;
			try {
				 ts = new StopFilter( Version.LUCENE_36,
			              new StandardTokenizer(Version.LUCENE_36,
			              new StringReader(pharse)),
			              StopAnalyzer.ENGLISH_STOP_WORDS_SET
			    );
			    				    
				    CharTermAttribute termAtt =
					          ts.getAttribute(CharTermAttribute.class);
					while(ts.incrementToken()) {
						 String eachToken = termAtt.toString();
						 filteredPhraseList.add(eachToken);
					}
					ts.close();
				}
			    catch(Exception ex)
				{
				} 
				finally 
				{
					  if(ts != null){
					    try {
					      ts.close();
					    } catch (Exception e) {}
					}
				}
					
				StringBuffer sb = new StringBuffer();
				for(String eachWord : filteredPhraseList)
				{
					sb.append(eachWord);
					sb.append(" ");
				}
				CommonUtil.logMessage(logger, currentThreadName, "filterPhrase: exiting with %s", sb.toString());
				return sb.toString();
					
	}
	
	/* 
	 * Eliminate stop words and punctuation like comma , double quotes - use Lucene.
	 * Return back filtered phrase in a tokenized list.
	 */
	public static List<String> filterPhrase2(String pharse)
	{	
		List<String> filteredPhraseList = new ArrayList<>();

		String currentThreadName = Thread.currentThread().getName();

		CommonUtil.logMessage(logger, currentThreadName, "filterPhrase2: entering with %s", pharse);		
		 TokenStream ts = null;
			try {
				 ts = new StopFilter( Version.LUCENE_36,
			              new StandardTokenizer(Version.LUCENE_36,
			              new StringReader(pharse)),
			              StopAnalyzer.ENGLISH_STOP_WORDS_SET
			    );
			    				    
				    CharTermAttribute termAtt =
					          ts.getAttribute(CharTermAttribute.class);
					while(ts.incrementToken()) {
						 String eachToken = termAtt.toString();
						 filteredPhraseList.add(eachToken);
					}
					ts.close();
				}
			    catch(Exception ex)
				{
				} 
				finally 
				{
					  if(ts != null){
					    try {
					      ts.close();
					    } catch (Exception e) {}
					}
				}
					
				
				CommonUtil.logMessage(logger, currentThreadName, "filterPhrase2: exiting with %s", filteredPhraseList);
				return filteredPhraseList;
					
	}

}
