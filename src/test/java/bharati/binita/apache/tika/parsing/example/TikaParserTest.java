package bharati.binita.apache.tika.parsing.example;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import backtype.storm.tuple.Values;

/**
 * 
 * @author binita.bharati@gmail.com
 *
 */

public class TikaParserTest {
	
	private static Logger logger = LoggerFactory.getLogger(TikaParserTest.class);

	
	public static void main(String[] args) {
		String[] urls = {"http://t.co/hP5PM6fm", "http://t.co/xSFteG23"};
		for (String url : urls)
		{
			try {
		          Parser parser = new AutoDetectParser();
		          Metadata metadata = new Metadata();
		          ParseContext parseContext = new ParseContext();
		          URL urlObject = new URL(url);
		          ContentHandler handler = new BodyContentHandler(10 *
		                                   1024 * 1024);
		          parser.parse((InputStream) urlObject.getContent(),
		                         handler, metadata, parseContext);
		          String[] mimeDetails = metadata.get("Content-Type")
		                                              .split(";");
		          logger.info("execute: url = "+url+", mimeDetails = "+Arrays.asList(mimeDetails));
		          
		          
		        } catch (Exception ex) {
		        	ex.printStackTrace();
		        }
		}
        
	}

}
