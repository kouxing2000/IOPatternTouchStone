package com.shark.iopattern.touchstone.agent;

import java.util.Map;

public interface Service {
	
	void start() throws Exception;
	
	void stop() throws Exception;
	
	void clearCounters();
	
	boolean isRunning();
	
	String getStatusInfo();
	
	SystemLoad getSystemLoad();
	
	Report getReport();
	
	String getParameters();
	
	void set(Map<String,String> parameters) throws Exception;
	
	void rollbackPreviousSet() throws Exception;
	
}
