/**
 * 
 */
package com.shark.iopattern.touchstone.agent;

/**
 * @author weili1
 *
 */
public class IndexItem {
	
	private double throughput;

	private double averageLatency;

	private double mostBelowLatency;
	
	private double serverCPUUsage;
	
	private double clientCPUUsage;

	public double getThroughput() {
		return throughput;
	}

	public void setThroughput(double throughput) {
		this.throughput = throughput;
	}

	public double getAverageLatency() {
		return averageLatency;
	}

	public void setAverageLatency(double latency) {
		this.averageLatency = latency;
	}

	public double getMostBelowLatency() {
		return mostBelowLatency;
	}
	
	public void setMostBelowLatency(double mostBelowLatency) {
		this.mostBelowLatency = mostBelowLatency;
	}
	
	public double getServerCPUUsage() {
		return serverCPUUsage;
	}

	public void setServerCPUUsage(double serverCPUUsage) {
		this.serverCPUUsage = serverCPUUsage;
	}

	public double getClientCPUUsage() {
		return clientCPUUsage;
	}

	public void setClientCPUUsage(double clientCPUUsage) {
		this.clientCPUUsage = clientCPUUsage;
	}

	@Override
	public String toString() {
		return "["
				+ "\n throughput=" + throughput 
				+ "\n averageLatency=" + averageLatency
				+ "\n mostBelowLatency="	+ mostBelowLatency 
				+ "\n serverCPUUsage=" + serverCPUUsage
				+ "\n clientCPUUsage=" + clientCPUUsage 
				+ "]";
	}
	
}
