package com.anshul.processor;

import java.util.Arrays;
import java.util.HashMap;

//import java.util.List;
//
//import org.apache.storm.cassandra.bolt.BaseCassandraBolt;
//import org.apache.storm.cassandra.executor.AsyncResultHandler;
//import org.apache.storm.cassandra.executor.impl.SingleAsyncResultHandler;
//import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
///*import org.apache.storm.cassandra.bolt.BaseCassandraBolt;
//import org.apache.storm.cassandra.executor.AsyncResultHandler;
//import org.apache.storm.cassandra.executor.impl.SingleAsyncResultHandler;
//import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
//import org.apache.storm.tuple.Tuple;*/
//import org.apache.storm.tuple.Tuple;
//
//import com.datastax.driver.core.Statement;

/**
* @author ansdhyan
*
*/
//public class CassandraWriterBolt extends BaseCassandraBolt<Tuple>{
//
//	private static final long serialVersionUID = -20147556033869461L;
//	private AsyncResultHandler<Tuple> asyncResultHandler; 
//	
//	public CassandraWriterBolt(CQLStatementTupleMapper tupleMapper) {
//		super(tupleMapper);
//	}
//
//	@Override
//	protected AsyncResultHandler<Tuple> getAsyncHandler() {
//		 if( asyncResultHandler == null) { 
//	            asyncResultHandler = new SingleAsyncResultHandler(getResultHandler()); 
//	        } 
//	        return asyncResultHandler;
//	}
//
//	@Override
//	protected void process(Tuple input) {
//		 List<Statement> statements = getMapper().map(stormConfig, session, input); 
//	        if (statements.size() == 1) getAsyncExecutor().execAsync(statements.get(0), input); 
//	        else getAsyncExecutor().execAsync(statements, input); 
//		
//	}
//
//}

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.base.Preconditions;

public class CassandraWriterBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5183625636743376774L;
	static final Logger logger = LoggerFactory.getLogger(CassandraWriterBolt.class);
    OutputCollector outputCollector;
    Cluster cassandraCluster;
    Session session;
    String component;
    Map<String,String> config;
    
//    String cql;
    String cql = "insert into tablename.keyspace (name,age,message) values (?,?,?);";

    public static Session getSessionWithRetry(Cluster cluster, String keyspace) {
        while (true) {
            try {
                return cluster.connect(keyspace);
            } catch (NoHostAvailableException e) {
                logger.warn("All Cassandra Hosts offline. Waiting to try again.");
                Utils.sleep(1000);
            }
        }

    }

    public static Cluster setupCassandraClient(String nodes) {
        return Cluster.builder()
                .withoutJMXReporting()
                .withoutMetrics()
                .addContactPoint(nodes)
//                .addContactPoints(nodes)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, TimeUnit.MINUTES.toMillis(5)))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        config = new HashMap<>();
        
        
        config.put(StormCassandraConstants.CASSANDRA_HOST, "127.0.0.1:9042");
        config.put(StormCassandraConstants.CASSANDRA_KEYSPACE, "[testkeyspace]");
        
        
        cassandraCluster = setupCassandraClient(config.get("cassandra.host"));
        session = CassandraWriterBolt.getSessionWithRetry(cassandraCluster,config.get("cassandra.keyspace"));

//        PreparedStatement pd = ne

        //We encode the cql for this bolt in the config under the component name
        component = topologyContext.getThisComponentId();
        cql = config.get(component+".cql");

        Preconditions.checkArgument(cql != null, "CassandraWriterBolt:"+component+" is missing a cql statement in bolt config");
    }

    @Override
    public void execute(Tuple input) {

        try {

            PreparedStatement stmt = session.prepare(cql);
            BoundStatement bound = stmt.bind(input.getValues().toArray());

            session.execute(bound);
            outputCollector.ack(input);
            //logger.info("Wrote to cassandra info about {}",component);

        } catch (Throwable t) {

            outputCollector.reportError(t);
            outputCollector.fail(input);

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //No outputs
    }
}

