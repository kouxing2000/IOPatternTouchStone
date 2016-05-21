package com.shark.iopattern.touchstone.server.shark;

import java.nio.ByteBuffer;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.Constants;
import com.shark.iopattern.touchstone.share.ProtocolSPI;


public final class LFServer extends AbstractServerWithWriter{

	private LFThread[] threads_ = null;

	public LFServer(ServerSPI serverSPI, ProtocolSPI protocolSPI) {

		this.serverSPI = serverSPI;
		this.protocolSPI = protocolSPI;

		threads_ = new LFThread[ServerConstants.THREAD_POOL_SIZE];
		for (int i = 0; i < threads_.length; i++) {
			String name = getClass().getSimpleName() + "-Thread[" + i + "]";
			threads_[i] = new LFThread(name);
		}
	}

	public synchronized void start() throws Exception{
		
		super.start();

		for (int i = 0; i < threads_.length; i++) {
			threads_[i].start();
		}

	}

	public synchronized void stop() throws Exception{
		super.stop();
		
	}

	private class LFThread extends Thread {

		LFThread(String name) {
			super(name);
		}

		public void run() {
			try {
				logger.info(this.getName() + ": started.");
				
				runningThreads.add(this);
				
				// init
                ByteBuffer sendBuffer = ByteBuffer
                		.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
                
                //ByteBuffer recvBuffer = ByteBuffer
                //		.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
                
                ByteBuffer[] recvBuffers = new ByteBuffer[ServerConstants.BATCH_PROCESS_NUMBER];
                
                for (int i = 0; i < recvBuffers.length; i++) {
                	recvBuffers[i] = ByteBuffer
                			.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
                }
                
                do {
                	
                	int batchSize = 0;
                	
                	boolean channelFailed = false;
                
                	BufferWriteChannelHolder channelHolder = readableChannelQueue.take();
                	
                	for (int i = 0; i < ServerConstants.BATCH_PROCESS_NUMBER; i++) {
                		
                		ByteBuffer recvBuffer = recvBuffers[i];
                		recvBuffer.clear();
                		
                		try{
                			// receive
                			if (!channelHolder.receiveMessage(recvBuffer)) {
                				logger.info("" + channelHolder + " read failed!");
                				channelFailed = true;
                				break;
                			}else{
                				batchSize++;
                			}
                		}catch (Exception e) {
                			logger.warn("", e);
                			channelFailed = true;
                			break;
                		}
                		
                		if (i < recvBuffers.length -1){
                			if (!channelHolder.isStillReadable()){
                				break;
                			}
                		}
                	}
                	
                	if (!channelFailed){
                		if (channelHolder.isStillReadable()) {
                			readableChannelQueue.put(channelHolder);
                		} else {
                			// need to register read key
                			channelHolder.beNotifiedWhenReadable();
                		}
                	}
                	
                	for (int i = 0; i < batchSize; i++) {
                
                		ByteBuffer recvBuffer = recvBuffers[i];
                
                		try {
                			// decode
                			// new response
                			// encode
                			serverSPI.process(recvBuffer, sendBuffer);
                			
                			// Send the response
                			if (synchronousWrite) {
                				channelHolder.sendMessage(sendBuffer);
                			} else {
                				if (channelHolder.appendSendBuffer(sendBuffer)) {
                					writeableChannelQueue.put(channelHolder);
                				}
                			}
                		} finally {
                			sendBuffer.clear();
                			//recvBuffer.clear();
                		}
                
                	}
                
                } while (started_);
			} catch (InterruptedException iex) {
				logger.info(this.getName() + ": interupted.");
				return;
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				logger.info(this.getName() + ": stoped.");
			}
			
		}
	}
}
