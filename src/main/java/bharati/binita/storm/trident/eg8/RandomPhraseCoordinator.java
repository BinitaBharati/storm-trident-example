package bharati.binita.storm.trident.eg8;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.storm.trident.util.CommonUtil;

import storm.trident.spout.ITridentSpout.BatchCoordinator;

/**
 * 
 * @author binita.bharati@gmail.com
 * Responsible for managing transactions. Like starting , commiting a trxn.
 * The coordinator for a TransactionalSpout runs in a single thread and indicates when batches
 * of tuples should be emitted and when transactions should commit. The Coordinator that you provide 
 * in a TransactionalSpout provides metadata for each transaction so that the transactions can be replayed
 *
 */

public class RandomPhraseCoordinator implements BatchCoordinator<String>, Serializable{//Object is the metadata type

	private static Logger logger = LoggerFactory.getLogger(RandomPhraseCoordinator.class);

	 /**
     * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
     */
	@Override
	public void success(long txid) {
		// TODO Auto-generated method stub
		String curThreadName = Thread.currentThread().getName();

		CommonUtil.logMessage(logger, curThreadName, "success: Successful Transaction = %d", txid);
		
	}

	@Override
	public boolean isReady(long txid) {
		// TODO Auto-generated method stub
		String curThreadName = Thread.currentThread().getName();

		CommonUtil.logMessage(logger, curThreadName, "isReady: Transaction = %d", txid);

		return true;
	}

	 /**
     * Release any resources from this coordinator.
     */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	/**
     * Create metadata for this particular transaction id which has never
     * been emitted before. The metadata should contain whatever is necessary
     * to be able to replay the exact batch for the transaction at a later point.
     * 
     * The metadata is stored in Zookeeper.
     * 
     * Storm uses the Kryo serializations configured in the component configuration 
     * for this spout to serialize and deserialize the metadata.
     * 
     * @param txid The id of the transaction.
     * @param prevMetadata The metadata of the previous transaction
     * @param currMetadata The metadata for this transaction the last time it was initialized.
     *                     null if this is the first attempt
     * @return the metadata for this new transaction
     */
	@Override
	public String initializeTransaction(long txid, String paramX1,
			String paramX2) {
		// TODO Auto-generated method stub
		String curThreadName = Thread.currentThread().getName();

		CommonUtil.logMessage(logger, curThreadName, "initializeTransaction, txid = %d", txid);
		
		return null;
		}


}
