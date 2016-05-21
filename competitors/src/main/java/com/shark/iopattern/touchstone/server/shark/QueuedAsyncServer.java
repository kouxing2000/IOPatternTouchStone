package com.shark.iopattern.touchstone.server.shark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.ChannelHolder;
import com.shark.iopattern.touchstone.share.Constants;
import com.shark.iopattern.touchstone.share.ProtocolSPI;


final public class QueuedAsyncServer extends AbstractServerWithWriter {

	private volatile boolean started_ = false;

	private QASThread[] processThreads_ = null;

	private RecieverThread[] recvThreads_ = null;

	private BlockingQueue<Packet> dataQueue_ = null;
	
	public QueuedAsyncServer(ServerSPI serverSPI, ProtocolSPI protocolSPI) {

		this.serverSPI = serverSPI;
		this.protocolSPI = protocolSPI;

		dataQueue_ = new LinkedBlockingQueue<Packet>(ServerConstants.QUEUE_SIZE);

		processThreads_ = new QASThread[ServerConstants.THREAD_POOL_SIZE];
		for (int j = 0; j < processThreads_.length; j++) {
			String name = getClass().getSimpleName() + "-ProcessThread[" + j + "]";
			processThreads_[j] = new QASThread(name);
		}

		recvThreads_ = new RecieverThread[ServerConstants.READ_THREAD_POOL_SIZE];
		for (int j = 0; j < recvThreads_.length; j++) {
			recvThreads_[j] = new RecieverThread(getClass().getSimpleName() + "-RecieverThread[" + j + "]");
		}

	}

	public void start() throws Exception {
		if (started_) {
			throw new IllegalStateException("Pool already started.");
		}

		super.start();

		// Start the processors.
		for (int i = 0; i < processThreads_.length; i++) {
			processThreads_[i].start();
			runningThreads.add(processThreads_[i]);
		}

		// Start the reciever.
		for (int j = 0; j < recvThreads_.length; j++) {
			recvThreads_[j].start();
			runningThreads.add(recvThreads_[j]);
		}
		
		started_ = true;
	}

	public void stop() throws Exception {
		started_ = false;

		super.stop();
	}

	private class QASThread extends Thread {

		QASThread(String name) {
			super(name);
		}

		public void run() {
			try {

				logger.info(this.getName() + ": started.");

				// init
				ByteBuffer sendBuffer = ByteBuffer
						.allocate(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

				Packet packet;
				do {
					// Take a packet and start processing it.
					packet = dataQueue_.take();

					BufferWriteChannelHolder channelHolder = (BufferWriteChannelHolder) packet.channelHolder_;
					
					for (ByteBuffer recvBuffer : packet.buffers_){
						// decode
						// new response
						// encode
						serverSPI.process(recvBuffer, sendBuffer);
	
						// Send the response.
						
						try {
							if (synchronousWrite) {
								channelHolder.sendMessage(sendBuffer);
							} else {
								if (channelHolder.appendSendBuffer(sendBuffer)) {
									writeableChannelQueue.put(channelHolder);
								}
							}
						} catch(IOException ioe) {
							logger.error("", ioe);
							channelHolder.close();
							break;
						} finally {
							sendBuffer.clear();
						}
					}
				} while (started_);

			} catch (InterruptedException ex) {
				logger.info(this.getName() + ": interuped.");
				return;
			} catch (Exception ex) {
				logger.error("", ex);
			} finally{
				logger.info(this.getName() + ": stoped.");
			}
		}
	}

	private class RecieverThread extends Thread {

		RecieverThread(String name) {
			super(name);
		}

		public void run() {
			try {
				
				logger.info(getName() + " started.");

				{
					int maxRead = ServerConstants.MAX_READ_TIMES_PER_THREAD_CHANNEL;

					ByteBuffer[] receiveBuffers = new ByteBuffer[ServerConstants.BATCH_PROCESS_NUMBER];
					
					next_channel: do {

						BufferWriteChannelHolder processor = readableChannelQueue.take();

						int readNums = 0;
						
						int batchSize = 0;

						while (true) {

							ByteBuffer recvbuffer = ByteBuffer
									.allocate(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

							try {
								// receive
								if (!processor.receiveMessage(recvbuffer)) {
									logger.info("" + processor
											+ " read failed!");
									continue next_channel;
								}
							} catch (Exception e) {
								logger.warn("", e);
								
								continue next_channel;
							}

							readNums++;
							
							receiveBuffers[batchSize++] = recvbuffer;

							boolean needExitLoop = false;

							if (processor.isStillReadable()) {
								if (readNums < maxRead) {
									// continue read
								} else {
									// for fair, give other channels chance to
									// read bytes
									// from them
									readableChannelQueue.put(processor);
									needExitLoop = true;
								}
							} else {
								// need to register read key
								processor.beNotifiedWhenReadable();
								needExitLoop = true;
							}
							
							if (batchSize == ServerConstants.BATCH_PROCESS_NUMBER || (needExitLoop && batchSize > 0)){
								
								Packet packet = new Packet();
								
								packet.buffers_ = Arrays.copyOf(receiveBuffers, batchSize);
								
								packet.channelHolder_ = processor;

								if (logger.isDebugEnabled()) {
									logger.debug("Recevie : " + packet);
								}

								dataQueue_.put(packet);
								
								batchSize = 0;

							}
							
							if (needExitLoop){
								break;
							}

						}

					} while (started_);

				}
			} catch (InterruptedException ex) {
				logger.info(this.getName() + ": interuped.");
				return;
			} catch (Exception ex) {
				logger.error("", ex);
			} finally{
				logger.info(this.getName() + ": stoped.");
			}
		}
	}

	private static class Packet {

		ByteBuffer[] buffers_;
		ChannelHolder channelHolder_;

	}
}
