/**
 * 
 */
package com.shark.iopattern.touchstone.share;

import java.nio.ByteBuffer;

/**
 * @author weili1
 *
 */
public interface ProtocolSPI {
	
	int headerLength();

	int packageLength(ByteBuffer recvBuffer);
	
	int getRequestID(ByteBuffer buffer);
	
}
