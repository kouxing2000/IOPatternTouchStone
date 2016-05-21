/**
 * 
 */
package com.shark.iopattern.touchstone.share;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author weili1
 *
 */
public interface ChannelHolder {

	ByteBuffer receiveMessage() throws IOException;
	
	/**
	 * no thread safe guarantee
	 * 
	 * @param buffer
	 * @return
	 * @throws IOException
	 */
	boolean receiveMessage(ByteBuffer buffer) throws IOException;
	
	/**
	 * thread safe guarantee
	 * 
	 * @param buffer
	 * @throws IOException
	 */
	void sendMessage(ByteBuffer buffer) throws IOException;
	
	boolean isStillReadable();

	void close();
	
}
