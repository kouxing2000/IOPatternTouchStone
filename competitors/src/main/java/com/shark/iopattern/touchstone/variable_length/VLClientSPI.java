package com.shark.iopattern.touchstone.variable_length;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import com.shark.iopattern.touchstone.client.ClientConstants;
import com.shark.iopattern.touchstone.client.ClientSPI;


public class VLClientSPI implements ClientSPI {

	byte[] requestBytes; // = "0123456789abcdefghijklmnopqrstuvwxyz";

	Random random = new Random();

	public VLClientSPI() {
		init();
	}
	
	public void init(){
		requestBytes = new byte[ClientConstants.REQUEST_SIZE - 8];
		random.nextBytes(requestBytes);
	}

	@Override
	public ByteBuffer createNewMessage(int requestId) {
		ByteBuffer byteBuffer = ByteBuffer
				.allocate(ClientConstants.REQUEST_SIZE);
		createNewMessage(requestId, byteBuffer);
		return byteBuffer;
	}

	@Override
	public void createNewMessage(int requestId, ByteBuffer buffer) {
		buffer.position(8);
		buffer.put(requestBytes);
		buffer.flip();
		buffer.putInt(buffer.limit());
		buffer.putInt(requestId);
		buffer.position(0);
	}

	@Override
	public boolean validateAnswer(ByteBuffer buffer) {
		buffer.position(8);
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		boolean result = Arrays.equals(requestBytes, bytes);
		buffer.position(0);
		return result;
	}
}