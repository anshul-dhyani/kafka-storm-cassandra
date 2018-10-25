/**
 * 
 */
package com.anshul.processor;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;


/**
 * @author ansdhyan
 *
 */
public class KafkaStormCassandraTopology {

	  private static final String KAFKA_SPOUT_ID = "kafka-sentence-spout";
	  private static final String CONVERT_BOLT_ID = "convert-bolt";
	  private static final String TOPOLOGY_NAME = "kafka-cassandra-demo-topology";
	  private static final String CASSANDRA_BOLT = "WORD_COUNT_CASSANDRA_BOLT";

	  public static void main(String[] args) throws Exception {
	    int numSpoutExecutors = 1;

	    KafkaSpout kspout = buildKafkaSentenceSpout();
	    ConvertBolt convertBolt = new ConvertBolt();

	// local run
	    LocalCluster cluster = new LocalCluster();
	    
	    TopologyBuilder builder = new TopologyBuilder();
	    
	    builder.setSpout(KAFKA_SPOUT_ID, kspout, numSpoutExecutors);
	    builder.setBolt(CONVERT_BOLT_ID, convertBolt).globalGrouping(KAFKA_SPOUT_ID);

	// configure cassandra 
	    Config cfg = new Config();
	    String configKey = "cassandra-config";
	    HashMap<String, Object> clientConfig = new HashMap<String, Object>();
	    clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "127.0.0.1:9042");
	    clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {"testkeyspace"}));
	    cfg.put(configKey, clientConfig);

	// create a CassandraBolt that writes to the "stormcf" column
	// family and uses the Tuple field "word" as the row key
//	    CassandraBatchingBolt<String, String, String> cassandraBolt = new CassandraBatchingBolt<String, String, String>(configKey, new DefaultTupleMapper("testkeyspace", "meter_data", "word"));
//	    cassandraBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);

//	    builder.setBolt(CASSANDRA_BOLT, cassandraBolt).shuffleGrouping(REPORT_BOLT_ID);

//	    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
	    
	    CassandraWriterBolt cassandraWriterBolt = new CassandraWriterBolt();
//	    builder.setBolt(CASSANDRA_BOLT, cassandraWriterBolt).shuffleGrouping(CONVERT_BOLT_ID);
	    
	    
	// local run
	    cluster.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
	  }

	  private static KafkaSpout buildKafkaSentenceSpout() {
	    String zkHostPorts = "localhost:2181";
	    String topic = "test-topic";

	    String zkRoot = "/acking-kafka-sentence-spout";
	    String zkSpoutId = "acking-sentence-spout";
	    ZkHosts zkHosts = new ZkHosts(zkHostPorts);
	    
	    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
//	    spoutCfg.scheme = new RawMultiScheme();
	    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
	    return kafkaSpout;
	  }

}