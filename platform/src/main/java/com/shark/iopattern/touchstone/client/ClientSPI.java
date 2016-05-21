/**
 * 
 */
package com.shark.iopattern.touchstone.client;

import java.nio.ByteBuffer;

/**
 * @author weili1
 * 
 */
public interface ClientSPI {

	void init();

	ByteBuffer createNewMessage(int requestId);

	void createNewMessage(int requestId, ByteBuffer buffer);

	boolean validateAnswer(ByteBuffer buffer);

}
