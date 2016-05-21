package com.shark.iopattern.touchstone.agent;

import java.lang.management.ManagementFactory;

/**
 * @author weili1
 * 
 */
public class CPUMonitor {

	private long prevProcessCpuTime;

	private long prevUpTime;

	private int nCPUs;

	public CPUMonitor() {

		prevUpTime = getUptime();

		prevProcessCpuTime = getProcessCpuTime();

		nCPUs = ManagementFactory.getOperatingSystemMXBean()
				.getAvailableProcessors();
		
	}

	public synchronized double getUsage() {
		return Math.min(0.99F, getTotalUsage() / nCPUs);
	}
	
	public synchronized double getTotalUsage() {


		long upTime = getUptime();

		long processCpuTime = getProcessCpuTime();

		long elapsedCpu = processCpuTime - prevProcessCpuTime;
		long elapsedTime = upTime - prevUpTime;

		prevUpTime = upTime;
		prevProcessCpuTime = processCpuTime;
		
		if (elapsedTime == 0){
			return 0;
		}

		// cpuUsage could go higher than 100% because elapsedTime
		// and elapsedCpu are not fetched simultaneously. Limit to
		// 99% to avoid Plotter showing a scale from 0% to 200%.
		float cpuUsage = elapsedCpu
				/ (elapsedTime * 1000000F);

		return cpuUsage;
	}

	private long getUptime() {
		return ManagementFactory.getRuntimeMXBean().getUptime();
	}

	private long getProcessCpuTime() {
		return ((com.sun.management.OperatingSystemMXBean) ManagementFactory
				.getOperatingSystemMXBean()).getProcessCpuTime();
	}

	public static void main(String[] args) throws InterruptedException {}

}
