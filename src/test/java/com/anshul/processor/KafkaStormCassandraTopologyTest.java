/**
 * 
 */
package com.anshul.processor;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

/**
 * @author ansdhyan
 *
 */

public class KafkaStormCassandraTopologyTest {
	private static final String KAFKA_SPOUT_ID = "kafka-sentence-spout";
	private static final String CONVERT_BOLT_ID = "convert-bolt";
	private static final String TOPOLOGY_NAME = "kafka-cassandra-demo-topology";
	private static final String CASSANDRA_BOLT = "WORD_COUNT_CASSANDRA_BOLT";
	@Test
	public void testTopology(){

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		
		 FeederSpout feederSpout = new FeederSpout(new Fields());
		 ConvertBolt convertBolt = new ConvertBolt();
		 CassandraWriterBolt cassandraWriterBolt = new CassandraWriterBolt();
		    
		TopologyBuilder builder = new TopologyBuilder();
	    
		
		feederSpout.feed(new Values("{\"name\":\"Student1\",\"age\":12,\"message\":\"First student\"}"));
		
		
	    builder.setSpout(KAFKA_SPOUT_ID, feederSpout);
	    builder.setBolt(CONVERT_BOLT_ID, convertBolt).shuffleGrouping(KAFKA_SPOUT_ID);
	    builder.setBolt(CASSANDRA_BOLT, cassandraWriterBolt).shuffleGrouping(CONVERT_BOLT_ID);

	// configure cassandra 
	    Config cfg = new Config();
	    String configKey = "cassandra-config";
	    HashMap<String, Object> clientConfig = new HashMap<String, Object>();
	    clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
	    clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {"testkeyspace"}));
	    cfg.put(configKey, clientConfig);

	    
	// local run
	    cluster.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
	  }

	  
		
//		cluster.submitTopology("test", conf, builder.createTopology());
//		Utils.sleep(10000);
//		cluster.killTopology("test");
//		cluster.shutdown();

		  }


