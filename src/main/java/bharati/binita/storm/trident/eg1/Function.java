package bharati.binita.storm.trident.eg1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;
import bharati.binita.storm.trident.util.CommonUtil;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class Function extends BaseFunction{
	
	private static Logger logger = LoggerFactory.getLogger(Function.class);


	@Override
	public void execute(TridentTuple tuple,
			TridentCollector collector) {
		// TODO Auto-generated method stub
		String currentThreadName = Thread.currentThread().getName();
		TupleDS tupleDs = (TupleDS)tuple.getValueByField("myTuple");
		CommonUtil.logMessage(logger, currentThreadName, "execute: entered with tuple %s", tupleDs);
		
		int field1 = tupleDs.getField1();
		int field2 = tupleDs.getField2();

		
		if(field1 == 0 && field2 == 0)
		{
			/**
			 * Transaction is a success. Means the entire batch should contain tuples which fulfill this criteria.
			 */
			
		}
		else if(field1 == 0 && field2 == 1)
		{
			/**
			 * Fail the transaction.
			 */
			throw new FailedException("Throwing FAILEDEXCEPTION");
		}
		
	}

}
