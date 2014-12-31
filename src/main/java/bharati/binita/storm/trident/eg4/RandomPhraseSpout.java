package bharati.binita.storm.trident.eg4;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import bharati.binita.storm.trident.util.CommonUtil;
import storm.trident.spout.ITridentSpout;

/**
 * 
 * @author binita.bharati@gmail.com
 * https://storm.incubator.apache.org/documentation/Trident-state
 * http://storm.incubator.apache.org/documentation/Trident-spouts.html
 * 
 * ITridentSpout: The most general API that can support transactional or opaque transactional semantics. 
 *
 */

public class RandomPhraseSpout implements ITridentSpout<Object>{
	private static Logger logger = LoggerFactory.getLogger(RandomPhraseSpout.class);

	private RandomPhraseCoordinator coordinator = new RandomPhraseCoordinator();
	private RandomPhraseEmitter emitter;

	@Override
	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	 /**
     * The coordinator for a TransactionalSpout runs in a single thread and indicates when batches
     * of tuples should be emitted and when transactions should commit. The Coordinator that you provide 
     * in a TransactionalSpout provides metadata for each transaction so that the transactions can be replayed.
     */

	@Override
	public storm.trident.spout.ITridentSpout.BatchCoordinator<Object> getCoordinator(
			String arg0, Map arg1, TopologyContext arg2) {
		// TODO Auto-generated method stub
		return coordinator;
	}

	 /**
     * The emitter for a TransactionalSpout runs as many tasks across the cluster. Emitters are responsible for
     * emitting batches of tuples for a transaction and must ensure that the same batch of tuples is always
     * emitted for the same transaction id.
     */    

	@Override
	public storm.trident.spout.ITridentSpout.Emitter<Object> getEmitter(
			String arg0, Map conf, TopologyContext arg2) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();
		CommonUtil.logMessage(logger, currentThreadName, "getEmitter: entered with conf = %s", conf);
		Integer phraseCount = Integer.parseInt((String)conf.get("phraseCount"));
		emitter = new RandomPhraseEmitter(phraseCount);
		return emitter;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("randomPhrase");
	}
	
	

}
