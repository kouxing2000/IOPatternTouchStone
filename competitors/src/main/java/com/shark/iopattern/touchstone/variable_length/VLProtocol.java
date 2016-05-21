/**
 * 
 */
package com.shark.iopattern.touchstone.variable_length;

import java.nio.ByteBuffer;

import com.shark.iopattern.touchstone.share.ProtocolSPI;


/**
 * @author weili1
 *
 */
public class VLProtocol implements ProtocolSPI {

	@Override
	public int getRequestID(ByteBuffer buffer) {
		return buffer.getInt(4);
	}

	@Override
	public int headerLength() {
		return 8;
	}

	@Override
	public int packageLength(ByteBuffer recvBuffer) {
		return recvBuffer.getInt();
	}

}
