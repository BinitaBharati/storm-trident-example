package bharati.binita.apache.lucene.example;

import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.Arrays;

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

import backtype.storm.tuple.Values;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;


public class LuceneStopFilterTest {
	
	private static Logger logger = LoggerFactory.getLogger(LuceneStopFilterTest.class);
	
	public static void main(String[] args) {
		
		String[] urls = {"http://t.co/hP5PM6fm", "http://t.co/xSFteG23"};
		for (String url : urls)
		{
			try {
				  //Parse the document content with Apache Tika into a string to be fed to the Lucene StopFilter - start
				  // https://lucidworks.com/blog/content-extraction-with-tika/
		          Parser parser = new AutoDetectParser();
		          Metadata metadata = new Metadata();
		          ParseContext parseContext = new ParseContext();
		          URL urlObject = new URL(url);
		          ContentHandler handler = new BodyContentHandler(10 *
		                                   1024 * 1024);
		          parser.parse((InputStream) urlObject.getContent(),
		                         handler, metadata, parseContext);		         
		          //Parse the document content into a string to be fed to the Lucene StopFilter - end
		          
		          //Print parsed document contents.
		          logger.info("************************************************************************************************");
		          String documentContents = handler.toString();
		          logger.info("LuceneStopFilterTest: documentContents = "+documentContents);
		          logger.info("************************************************************************************************");

		          
		          /*
		           * Tokenize the document content stream by passing it through a stop filter. This will eliminate useless words like:
		             { "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", 
		             "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", 
		             "this", "to", "was", "will", "with" }
		             
		             Lucene StopFilter wont work if the version of lucene jars in CP doesnt tally with the version provided
		             in the StopFilter constructor.
		             
		             The work done by Lucene StopFilter is done by another class called StandardAnalyzer, which eliminates stop words
		             and ignores case.

		           */
				  TokenStream ts = null;
					try {
					    ts = new StopFilter( Version.LUCENE_36,
					              new StandardTokenizer(Version.LUCENE_36,
					              new StringReader(documentContents)),
					              StopAnalyzer.ENGLISH_STOP_WORDS_SET
					    );
					    
					
					 CharTermAttribute termAtt =
					          ts.getAttribute(CharTermAttribute.class);
					 while(ts.incrementToken()) {
						 String eachToken = termAtt.toString();
						 logger.info("LuceneStopFilterTest: each token = "+eachToken);
				         logger.info("************************************************************************************************");
				          
				         /**
				          * A lemma is the canonical form of a word; for example, run, runs, ran, and running are forms of the same lexeme with "run" as the lemma. 
				          * Lexeme, in this context, refers to the set of all the forms that have the same meaning, and lemma refers to the particular form that is 
				          * chosen by convention to represent the lexeme.
				          */
					      String lemma = MorphaStemmer.stemToken(eachToken);
					      lemma = lemma.trim().replaceAll("\n","").replaceAll("\r", "");
						  logger.info("LuceneStopFilterTest: token = "+eachToken+", lemma = "+lemma);
					      logger.info("************************************************************************************************");


					 }
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
}
