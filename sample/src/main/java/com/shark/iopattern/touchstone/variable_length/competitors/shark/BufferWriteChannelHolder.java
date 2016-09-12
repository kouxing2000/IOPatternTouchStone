/**
 * 
 */
package com.shark.iopattern.touchstone.variable_length.competitors.shark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.ReentrantLock;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.share.Constants;
import com.shark.iopattern.touchstone.share.ProtocolSPI;


/**
 * @author weili1
 * 
 */
public class BufferWriteChannelHolder extends SelectionKeyChannelHolder {

	public BufferWriteChannelHolder(SelectionKey key, SocketChannel channel,
			ProtocolSPI protocolSPI) {
		super(key, channel, protocolSPI);
	}

	private ByteBuffer bufferedSendBuffer = ByteBuffer
			.allocate(ServerConstants.WRITE_BUFFER_SIZE + Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

	private ReentrantLock sendLock = new ReentrantLock();
	
	/**
	 * 
	 * @param buffer
	 * @return whether the send buffer is empty before this operation
	 */
	public boolean appendSendBuffer(ByteBuffer buffer) throws IOException{
	    boolean result = false;
	    
	    sendLock.lock();
	    
	    try{
	        result = bufferedSendBuffer.position() == 0;
	        bufferedSendBuffer.put(buffer);
	        if (bufferedSendBuffer.position() >= ServerConstants.WRITE_BUFFER_SIZE){
//	            if (logger.isInfoEnabled()){
//	                logger.info("Write Buffer of Channel " +
//	                        channel_ +
//	                        " is full, flush it by " + Thread.currentThread());
//	            }
	            flush();
	            return false;
	        }
		}finally{
		    sendLock.unlock();
		}
		
		return result;
	}

    public void flush() throws IOException {
        sendLock.lock();
        try {
            if (bufferedSendBuffer.position() > 0) {
                bufferedSendBuffer.flip();
                sendMessage(bufferedSendBuffer);
                bufferedSendBuffer.clear();
            }
        } finally {
            sendLock.unlock();
        }
    }
}
