/**
 * 
 */
package com.shark.iopattern.touchstone.share;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author weili1
 * 
 */
public class DefaultChannelHolder implements ChannelHolder {
    
	protected Logger logger = LoggerFactory
			.getLogger(getClass());

	protected final SocketChannel channel_;

	private final ByteBuffer bulkReadBuffer_ ;

	private long totalMessages_ = 0;

	private ProtocolSPI protocolSPI;
	
	private boolean closed;

	public DefaultChannelHolder(SocketChannel channel,
			ProtocolSPI protocolSPI, int readBufferSize) {
		this.protocolSPI = protocolSPI;
		channel_ = channel;
		bulkReadBuffer_ = ByteBuffer.allocateDirect(readBufferSize);
		bulkReadBuffer_.position(0);
		bulkReadBuffer_.limit(0);
	}

	@Override
	public ByteBuffer receiveMessage() throws IOException {

		ByteBuffer recvBuffer = ByteBuffer
				.allocate(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

		if (readData(recvBuffer)) {
			return recvBuffer;
		} else {
			return null;
		}

	}

	@Override
	public boolean receiveMessage(ByteBuffer recvBuffer) throws IOException {
	    
	    if (closed){
	        return false;
	    }
	    
	    if (!channel_.isOpen()){
	        return false;
	    }

		// Read the header.
		final int startPos = recvBuffer.position();

		int headerLength = protocolSPI.headerLength();

		recvBuffer.limit(startPos + headerLength);

		boolean channelClosed = false;
		try{
    		if (!this.readData(recvBuffer)) {
    		    channelClosed = true;
    		}
		}catch (Exception e) {
		    logger.info(Thread.currentThread().toString(), e);
		    channelClosed = true;
        }
		
		if (channelClosed){
		    logger.info(Thread.currentThread() + "Channel is Closed! :"
                    + channel_);
            logger.info(Thread.currentThread() + "Total Messages Recieved = "
                    + totalMessages_);
            
            close();

            return false;
		}
		
		recvBuffer.position(startPos);

		// Decode the length
		final int length = protocolSPI.packageLength(recvBuffer);

		recvBuffer.position(headerLength);

		try{
		    // Skip the header and read the body of the packet.
			recvBuffer.limit(length);
		}catch (Exception e) {
			logger.error("recvBuffer:" + recvBuffer + " length:"+ length, e);
			return false;
		}

		if (!this.readData(recvBuffer)) {
			throw new IOException("Socket closed in the middle of a packet.");
		}

		recvBuffer.position(startPos);
		
		if (logger.isDebugEnabled()){
			logger.debug("channel:" + channel_ + " read bytes:\n"
					+ BytesHexDumper.getHexdump(recvBuffer));
		}


		++totalMessages_;
		
		// logger.info("receive:" + totalMessages_);

		return true;
	}
	
	private boolean warnedWriteZeroOut;
	

	@Override
	public void sendMessage(ByteBuffer buffer) throws IOException {
	    
//		if (logger.isDebugEnabled()){
//			logger.debug("channel:" + channel_ + " write bytes:\n"
//					+ BytesHexDumper.getHexdump(buffer));
//		}
		
		int writeNum;
		do {
			writeNum = channel_.write(buffer);
			if (writeNum == 0 && !warnedWriteZeroOut){
				logger.warn("channel:" + channel_ + " write 0 bytes out, please check whether socke write buffer size :" +
						channel_.socket().getSendBufferSize() +
						" is too small!, continue trying write until finished");
				warnedWriteZeroOut = true;
			}
		} while (writeNum == 0);
		
		
	}
	
	@Override
	public boolean isStillReadable() {
		return bulkReadBuffer_.hasRemaining();
	}

	private boolean readData(ByteBuffer recvBuffer) throws IOException {

		// logger.info("[@] readData begin" + " bulkReadBuffer_" +
		// bulkReadBuffer_ + " recvBuffer" + recvBuffer);

		while (recvBuffer.hasRemaining()) {
			if (bulkReadBuffer_.hasRemaining()) {
				int srcLimit = bulkReadBuffer_.limit();
				int srcPos = bulkReadBuffer_.position();
				int dstRemaining = recvBuffer.remaining();
				int srcRemaining = (srcLimit - srcPos);
				if (srcRemaining > dstRemaining) {
					bulkReadBuffer_.limit(srcPos + dstRemaining);
					recvBuffer.put(bulkReadBuffer_);
					bulkReadBuffer_.limit(srcLimit);
				} else {
					recvBuffer.put(bulkReadBuffer_);
				}
			} else {
				bulkReadBuffer_.clear();
				int bytesRead = 0;
				do {
					bytesRead = channel_.read(bulkReadBuffer_);
					if (bytesRead < 0) {
						bulkReadBuffer_.position(0);
						bulkReadBuffer_.limit(0);

						return false;
					}
				} while (bytesRead == 0);
				bulkReadBuffer_.flip();
			}
		}

		// logger.info("[@] readData finish" + " bulkReadBuffer_" +
		// bulkReadBuffer_ + " recvBuffer" + recvBuffer);
		return true;
	}

	@Override
	public void close() {
		
		 logger.info("close channel:" + channel_);
		
		 this.closed = true;
		 
		 try {
			channel_.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public String toString() {
		return "" + this.channel_;
	}
	
	
}
