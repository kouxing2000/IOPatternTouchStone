/**
 * 
 */
package com.shark.iopattern.touchstone.client;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author weili1
 *
 */
public class PendingManager {
	
	private int max;

	public PendingManager(int max) {
		this.max = max;
	}
	
	private AtomicInteger counter = new AtomicInteger();
	
	private Object lock = new Object();
	
	private volatile boolean blocking;
	
	/**
	 * when current pending size > max, the calling thread will be paused, until counter value smaller than max value
	 * @return
	 */
	public int increase(){
		
		int result = counter.incrementAndGet();
		
		if (result > max){
			synchronized (lock) {
				blocking = true;
				try {
					lock.wait(2000);
					
					if (blocking){
						//means it is not notified
						System.err.println("@@, Have been waiting notification for 2 seconds!");
						System.err.println("@@, Previous counter:" + result + " Present counter:" + counter.get());
					}
					
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		return result;
	}
	
	public int decrease(){
		
		int result = counter.decrementAndGet();
		
		if (blocking && result < max){
			synchronized (lock) {
				blocking = false;
				lock.notifyAll();
			}
		}
		
		return result;
	
	}
	
	public int get(){
		return counter.get();
	}
	
}
