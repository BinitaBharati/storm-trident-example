package bharati.binita.apache.lucene.example;

import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import edu.washington.cs.knowitall.morpha.MorphaStemmer;

public class StopFilterTest2 {
	
private static Logger logger = LoggerFactory.getLogger(StopFilterTest2.class);
	
	public static void main(String[] args) {
		
		try {
				  
		          //Print parsed document contents.
		          logger.info("************************************************************************************************");
		          String documentContents = "They have the same men, no age problem, no injuries and they also have Vinegar Bend Mizell for the full season, along with Bobby Shantz.";
		          logger.info("LuceneStopFilterTest: documentContents = "+documentContents);
		          logger.info("************************************************************************************************");

		          
				  TokenStream ts = null;
					try {
					    ts = new StopFilter( Version.LUCENE_36,
					              new StandardTokenizer(Version.LUCENE_36,
					              new StringReader(documentContents)),
					              StopAnalyzer.ENGLISH_STOP_WORDS_SET
					    );
					    
					
					 CharTermAttribute termAtt =
					          ts.getAttribute(CharTermAttribute.class);
					 List<String> test = new ArrayList<>();
					 while(ts.incrementToken()) {
						 String eachToken = termAtt.toString();
						 logger.info("LuceneStopFilterTest: each token12 = "+eachToken);
				         logger.info("************************************************************************************************");
				         test.add(eachToken);
						  logger.info("LuceneStopFilterTest: test1 = "+test);

				         /**
				          * A lemma is the canonical form of a word; for example, run, runs, ran, and running are forms of the same lexeme with "run" as the lemma. 
				          * Lexeme, in this context, refers to the set of all the forms that have the same meaning, and lemma refers to the particular form that is 
				          * chosen by convention to represent the lexeme.
				          */
					     /* String lemma = MorphaStemmer.stemToken(eachToken);
					      lemma = lemma.trim().replaceAll("\n","").replaceAll("\r", "");
						  logger.info("LuceneStopFilterTest: token = "+eachToken+", lemma = "+lemma);
					      logger.info("************************************************************************************************");
*/

					 }
					 logger.info("LuceneStopFilterTest: test2 = "+test);
					 ts.close();
					}
					catch(Exception ex)
					{
					} 
					finally {
						  if(ts != null){
						    try {
						      ts.close();
						    } catch (Exception e) {}
						}
					}
		          
		          
		        } catch (Exception ex) {
		        	ex.printStackTrace();
		        }
		
		
	}

}
