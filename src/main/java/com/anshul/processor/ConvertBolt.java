/**
 * 
 */
package com.anshul.processor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author ansdhyan
 *
 */
public class ConvertBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1706066375151440171L;
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector outCollector) {
		collector = outCollector;
	}

	public void execute(Tuple input) {
		System.out.println("************************ Input to ConvertBolt : " + input);
		System.out.println("************************ Input(o) : " + input.getValue(0));
//		System.out.println("************************ Input(1) : " + input.getValue(1));
//		System.out.println("************************ Input(2) : " + input.getValue(2));
//		System.out.println("************************ Input(3) : " + input.getValue(3));

		Object value = input.getValue(0);
		String message = null;
		if (value instanceof String) {
			message = (String) value;
		} else {
			// Kafka returns bytes
			byte[] bytes = (byte[]) value;
			try {
				message = new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
		try {
			Student student = new ObjectMapper().readValue(message, Student.class);
			student.setAge(-1);
			System.out.println("************************ Output of ConvertBolt : " + student.toString());
			collector.emit(new Values(student));
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("x"));

	}

}
