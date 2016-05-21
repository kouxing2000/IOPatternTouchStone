/**
 * 
 */
package com.shark.iopattern.touchstone.server;

import java.nio.ByteBuffer;

/**
 * @author weili1
 *
 */
public interface ServerSPI {

	void process(ByteBuffer in, ByteBuffer out);
	
}
