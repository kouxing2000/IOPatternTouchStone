/**
 * 
 */
package com.shark.iopattern.touchstone.variable_length.run;


import com.shark.iopattern.touchstone.agent.Commander;
import com.shark.iopattern.touchstone.client.PerfClient;
import com.shark.iopattern.touchstone.server.PerfServer;

/**
 * @author weili1
 * 
 */
public class PerfTotal {
	public static void main(String[] args) throws Throwable {

		PerfServer.main(args);
		
		PerfClient.main(args);
		
		Commander.main(new String[]{"parameters_test.json"});
		
		System.exit(0);

	}
	
}
