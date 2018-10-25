/**
 * 
 */
package com.anshul.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author ansdhyan
 *
 */
public class Demo {

	/**
	 * @param args
	 * @throws JsonProcessingException 
	 */
	public static void main(String[] args) throws JsonProcessingException {
		Student s1 = new Student("Student1", 12, "First student");
		Student s2 = new Student("Student2", 20, "Second student");
		
		ObjectMapper om = new ObjectMapper();
		System.out.println(om.writeValueAsString(s1));
		System.out.println(om.writeValueAsString(s2));
	}

}
