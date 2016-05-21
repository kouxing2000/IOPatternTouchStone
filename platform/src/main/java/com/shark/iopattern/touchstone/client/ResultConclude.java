/**
 * 
 */
package com.shark.iopattern.touchstone.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

/**
 * @author weili1
 * 
 */
public class ResultConclude {

	private List<Long> results = new ArrayList<Long>();
	
	private volatile int addNum;

	public synchronized void add(long[] latencies, long numMessages) {
		for (int i = 0; i < numMessages; i++) {
			results.add(latencies[i]);
		}
		addNum++;
	}
	
	int getAddNum(){
		return this.addNum;
	}

	private double throughput;

	public double getThroughput() {
		return throughput;
	}

	public synchronized void clean() {
		results.clear();
		throughput = 0;
	}

	public void finish(double time) {
		Long[] latencies = results.toArray(new Long[0]);
		long maxLatency = generateLatencyMetrics(latencies, results.size(),
				time / 1000d);
		generateHistogram(latencies, maxLatency);
	}
	
	
	private double averageLatency;
	
	public double getAverageLatency() {
		return averageLatency;
	}
	
	private double mostBelowLatency;

	public double getMostBelowLatency() {
		return mostBelowLatency;
	}

	public String latencyMetrics;

	private long generateLatencyMetrics(Long[] latencies, long numMessages,
			double time) {
		
		Arrays.sort(latencies);
		
		mostBelowLatency = latencies[(int)(latencies.length * ClientConstants.MOST_DEFINITION)];
		
		StringBuilder builder = new StringBuilder();

		long totalLatency = 0;
		long minLatency = 0;
		long maxLatency = 0;
		for (int i = 0; i < numMessages; ++i) {
			long latency = (latencies[i]);
			if (latency < 0) {
				// Should never happen
				// Ignore this RUN
				System.err.println("Bad latency number: " + latency);
				return -1;
			}
			if (latency < minLatency) {
				minLatency = latency;
			}

			if (latency > maxLatency) {
				maxLatency = latency;
			}
			totalLatency += latency;
		}

		averageLatency = (totalLatency * 1.0d) / numMessages;
		// double time = (timer.getElapsedTimeInMillis() / 1000.0d);

		builder.append("Total Messages: " + numMessages);
		builder.append("\nMin latency   : " + minLatency + " msec");
		builder.append("\nMax latency   : " + maxLatency + " msec");

		Formatter formatter = new Formatter();
		formatter.format("\nAvg latency   : %.3f msec", new Object[] { Double
				.valueOf(averageLatency) });
		builder.append(formatter.out().toString());
		formatter = new Formatter();
		formatter.format("\nTotal time    : %.3f sec", new Object[] { Double
				.valueOf(time) });
		builder.append(formatter.out().toString());
		throughput = Double.valueOf(numMessages / time);
		formatter = new Formatter();
		formatter.format("\nThroughput    : %.3f req/sec",
				new Object[] { throughput });
		builder.append(formatter.out().toString());

		latencyMetrics = (builder.toString());

		return maxLatency;
	}
	
	public String histogram;

	private void generateHistogram(Long[] latencies, long maxLatency) {

		StringBuilder builder = new StringBuilder();

		int numBuckets = ClientConstants.LATENCY_BUCKETS;
		int bucketSize = ClientConstants.LATENCY_BUCKET_SIZE;
		int[] counters = new int[numBuckets];
		int[] totals = new int[numBuckets];
		for (int i = 0; i < latencies.length; ++i) {
			int latency = latencies[i].intValue();
			int correction = ((latency != 0) && (latency % bucketSize) == 0) ? 1
					: 0;
			int bucket = ((latency / bucketSize) - correction);
			if (bucket >= numBuckets) {
				bucket = (numBuckets - 1);
			}
			counters[bucket]++;
			totals[bucket] += latency;
		}

		for (int i = 0; i < numBuckets; ++i) {
			if (counters[i] > 0) {
				int max = (i * bucketSize) + bucketSize;
				int min = (i == 0) ? 0 : (max + 1 - bucketSize);
				double avg = (counters[i] == 0) ? 0
						: ((totals[i] * 1.0d) / counters[i]);
				double percent = ((counters[i] * 100.0d) / latencies.length);
				Formatter formatter = new Formatter();
				formatter.format("%8d", new Object[] { Integer
						.valueOf(counters[i]) });
				String counterStr = formatter.out().toString();
				formatter = new Formatter();
				formatter.format("%3.3f", new Object[] { Double.valueOf(avg) });
				String avgStr = formatter.out().toString();
				formatter = new Formatter();
				formatter.format("%3.3f", new Object[] { Double
						.valueOf(percent) });
				String percentStr = formatter.out().toString();

				if (i == (numBuckets - 1)) {
					// Last Bucket
					builder.append("\n Latency [" + min + " - * ]: "
							+ counterStr + " : " + avgStr + " - " + percentStr
							+ "%");
				} else {
					if (min == 0) {
						builder.append("\n Latency [ 0 - " + max + "]: "
								+ counterStr + " : " + avgStr + " - "
								+ percentStr + "%");
					} else {
						builder.append("\n Latency [" + min + " - " + max
								+ "]: " + counterStr + " : " + avgStr + " - "
								+ percentStr + "%");
					}
				}
			}
		}

		histogram = (builder.toString());
	}
}
