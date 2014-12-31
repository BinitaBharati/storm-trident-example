package bharati.binita.storm.trident.eg1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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

public class SpoutEmitter implements Emitter<Object>, Serializable{//Object is the type of the tuple that will be emitted
	
	private static Logger logger = LoggerFactory.getLogger(SpoutEmitter.class);
	private static int BATCH_SIZE = 4;
	private static Random rand = new Random();
	
	private static AtomicInteger successfulTransactions = new AtomicInteger(0);


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
		for (int i = 0; i < BATCH_SIZE; i++) {
	          List<Object> events = new ArrayList<Object>();
	          
	          String uid = UUID.randomUUID().toString();
	          int field1 = rand.nextInt(1);//possible values - 0
	          int field2 = rand.nextInt(1);//possible values - 0
	          
	          TupleDS tupleDs = new TupleDS(uid, uid, field1, field2);
	          
	          events.add(tupleDs);
	          collector.emit(events);
	  		  CommonUtil.logMessage(logger, curThreadName, "emitBatch: emitting tuple for trxnId = %d with uid = %s, field1 = %d, field2 = %d",  trxnId, uid, field1, field2);

		}
		
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
