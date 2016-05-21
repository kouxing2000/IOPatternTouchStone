/**
 * 
 */
package com.shark.iopattern.touchstone.agent;

import java.io.Serializable;


/**
 * @author weili1
 *
 */
public class SystemLoad implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3146479999836278978L;
	
	private double cpuUsage;

	public double getCpuUsage() {
		return cpuUsage;
	}

	public void setCpuUsage(double cpuUsage) {
		this.cpuUsage = cpuUsage;
	}
}
