/**
 * 
 */
package com.shark.iopattern.touchstone.agent;

import java.io.Serializable;

/**
 * @author weili1
 * 
 */
public class Report implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8254005727905877674L;

	private double throughput;

	private double averageLatency;
	
	private double mostBelowLatency;

	public double getThroughput() {
		return throughput;
	}

	public void setThroughput(double throughput) {
		this.throughput = throughput;
	}

	public double getAverageLatency() {
		return averageLatency;
	}

	public void setAverageLatency(double averageLatency) {
		this.averageLatency = averageLatency;
	}
	
	public double getMostBelowLatency() {
		return mostBelowLatency;
	}
	
	public void setMostBelowLatency(double mostBelowLatency) {
		this.mostBelowLatency = mostBelowLatency;
	}

	@Override
	public String toString() {
		return "Report [averageLatency=" + averageLatency
				+ ", mostBelowLatency=" + mostBelowLatency + ", throughput="
				+ throughput + "]";
	}
	
}
