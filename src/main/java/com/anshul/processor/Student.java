/**
 * 
 */
package com.anshul.processor;

import java.io.Serializable;

/**
 * @author ansdhyan
 *
 */
public class Student implements Serializable {

	private static final long serialVersionUID = 4296140699027394568L;
	private String name;
	private int age;
	private String message;
	
	
	
	/**
	 * default
	 */
	public Student() {
		super();
	}
	
	
	/**
	 * @param name
	 * @param age
	 * @param message
	 */
	public Student(String name, int age, String message) {
		this();
		this.name = name;
		this.age = age;
		this.message = message;
	}


	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return the age
	 */
	public int getAge() {
		return age;
	}
	/**
	 * @param age the age to set
	 */
	public void setAge(int age) {
		this.age = age;
	}
	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}
	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Student [name=" + name + ", age=" + age + ", message=" + message + "]";
	}
	
	
}
