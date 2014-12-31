package bharati.binita.storm.trident.eg3;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import bharati.binita.storm.trident.util.CommonUtil;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

/**
 * 
 * @author binita.bharati@gmail.com
 * Responsible for emitting batches of tuples per Spout trxn.
 *
 */

public class RandomPhraseEmitter implements Emitter<Object>, Serializable{//Object is the type of the tuple that will be emitted
	
	private static Logger logger = LoggerFactory.getLogger(RandomPhraseEmitter.class);
	private static Random rand = new Random();
	private int MAX_BATCH_SIZE = 10;
		
	/*private String[] sentences = {//world
								  "To be yourself in a world that is constantly trying to make you something else is the greatest accomplishment", 
								  "To live is the rarest thing in the world. Most people exist, that is all",
								  "Be the change that you wish to see in the world.",
								  "No matter what people tell you, words and ideas can change the world",
								  "Let us remember: One book, one pen, one child, and one teacher can change the world",
								  "Things don't have to change the world to be important",
								  "As a rock star, I have two instincts, I want to have fun, and I want to change the world. I have a chance to do both",
								  "It is more rewarding to watch money change the world than watch it accumulate",
								  "Never believe that a few caring people can't change the world. For, indeed, that's all who ever have",
								  //live
								  "You only live once, but if you do it right, once is enough",
								  "Live as if you were to die tomorrow. Learn as if you were to live forever",
								  "To live is the rarest thing in the world. Most people exist, that is all",
								  "I hated every minute of training, but I said, 'Don't quit. Suffer now and live the rest of your life as a champion",
								  "The world is a dangerous place to live; not because of the people who are evil, but because of the people who don't do anything about it",
								  "Life is short, live it. Love is rare, grab it. Anger is bad, dump it. Fear is awful, face it. Memories are sweet, cherish it"
								  //love
								  
								  };*/
	private String[] sentences;
		
	public RandomPhraseEmitter()
	{
		try {
			sentences = (String[]) IOUtils.readLines(
			    ClassLoader.getSystemClassLoader().getResourceAsStream("500_sentences_en.txt")).toArray(new String[0]);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
	}

	 /**
     * Emit a batch for the specified transaction attempt and metadata for the transaction. The metadata
     * was created by the Coordinator in the initializeTranaction method. This method must always emit
     * the same batch of tuples across all tasks for the same transaction id.
     * 
     * emitBatch will keep getting called repeatedly. Each invocation to emitBatch will be done in a new trxn.
     * emitBatch is internally invoked by Storm repeatedly, like a infinite while loop.Each iteration of the
     * loop is a trxn.Trxn Ids are auto-generated.
     * 
     */

	@Override
	public void emitBatch(TransactionAttempt trxnAttempt, Object coordinatorMeta,
			TridentCollector collector) {
		// TODO Auto-generated method stub
		
		String curThreadName = Thread.currentThread().getName();
		long trxnId = trxnAttempt.getTransactionId();


		CommonUtil.logMessage(logger, curThreadName, "emitBatch: entered with trxnId = %d",  trxnId);
				
		String randomPhrase = sentences[rand.nextInt(1)];
		CommonUtil.logMessage(logger, curThreadName, "emitBatch: randomPhrase = %s",  randomPhrase);
		
		List<Object> randomPhraseList = new ArrayList<>();
		randomPhraseList.add(randomPhrase);
		
		collector.emit(randomPhraseList);
			
	}

	 /**
     * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
     */

	@Override
	public void success(TransactionAttempt trxnAttempt) {
		// TODO Auto-generated method stub
		long trxnId = trxnAttempt.getTransactionId();
		String curThreadName = Thread.currentThread().getName();
		CommonUtil.logMessage(logger, curThreadName, "success: entered for trxnId = %d", trxnId);
		//successfulTransactions.incrementAndGet();
		//Util.logMessage(logger, curThreadName, "success: exiting with updated succesfulTrxns = %d", successfulTransactions.get());

	}

	/**
     * Release any resources held by this emitter.
     */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
