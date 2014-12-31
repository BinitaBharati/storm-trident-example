package bharati.binita.storm.trident.eg6;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;
import bharati.binita.storm.trident.util.CommonUtil;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import storm.trident.state.TransactionalValue;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.TransactionalMap;

/**
 * 
 * @author binita.bharati@gmail.com
 * https://storm.apache.org/documentation/Trident-state
 * 
 * Transactional state management
 * Same trxn id always carries same set of tuples. 
 * So, even when a purely transactional batch is replayed, same set of tuples should be present for that trxn as before.
 * 
 * https://groups.google.com/forum/#!msg/storm-user/TASr2zWyzKs/Tih81_UBiTQJ
 * 
 * TransactionalMap is a type of MapState.
 * TransactionalMap needs a instance of IBackingMap<TransactionalValue>.
 *
 */

public class RedisStoreState extends TransactionalMap{
	
	private static Logger logger = LoggerFactory.getLogger(RedisStoreState.class);

	
	public RedisStoreState(IBackingMap<TransactionalValue> backing) {
		super(backing);
		// TODO Auto-generated constructor stub
	}
	
	/*@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();
		Util.logMessage(logger, currentThreadName, "beginCommit: entered with txid = %d", txid);
		//super.beginCommit(txid);
	}
	
	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();
		Util.logMessage(logger, currentThreadName, "commit: entered with txid = %d", txid);
		//super.commit(txid);
	}
*/

}
