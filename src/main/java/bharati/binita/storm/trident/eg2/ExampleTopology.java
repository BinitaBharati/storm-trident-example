package bharati.binita.storm.trident.eg2;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * 
 * @author binita.bharati@gmail.com
 * http://metabroadcast.com/blog/testing-storm
 * https://storm.incubator.apache.org/documentation/Trident-state
 * http://svendvanderveken.wordpress.com/2014/02/05/error-handling-in-storm-trident-topologies/
 * http://metabroadcast.com/blog/testing-storm
 * 
 *  Illustrates the following:
 *  - Is a word counter.
 *  - State management  - via partitionPersist API.
 *  - Persistence of State. (In Trident, State can not be called as a 'State' without effective persistence)
 *  - Redis has been used as the persistence store.
 
 *
 */

public class ExampleTopology {
	
	public static StormTopology buildTopology()
	
	{
		TridentTopology topology = new TridentTopology();
		RandomWordSpout spout1 = new RandomWordSpout();
		
		Stream inputStream = topology.newStream("faltu", spout1);//faltu isnt used anywhere.
		
		/**
		 * partitionPersist : The partitionPersist operation updates a source of state.
		 * It returns a TridentState object. You could then use this state in stateQuery operations elsewhere in the topology.
		 * Args:
		 * StateFactory instance - This factory implement the makeState API, that should return a instance of State.
		 * Fields list, that needs to be persisted. These field list should be present in the input stream.
		 * StateUpdater instance - The StateUpdater instance will update the underlying State.
		 */
		 inputStream
		    .partitionPersist(new RedisStoreStateFactory(), new Fields("randomWord"), new RedisStoreStateUpdater());
		 
		 return topology.build();
	}

	public static void main(String[] args) throws Exception {
		
		Config conf = new Config();
		conf.put("redisServerIP", args[0]);
		conf.put("redisServerPort", args[1]);

		StormSubmitter.submitTopology("trident-eg2", conf,
        		buildTopology());
	
	}

}
