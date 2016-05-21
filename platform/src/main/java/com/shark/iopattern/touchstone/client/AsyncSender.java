package com.shark.iopattern.touchstone.client;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import com.shark.iopattern.touchstone.share.ChannelHolder;
import com.shark.iopattern.touchstone.share.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AsyncSender extends Thread {
	private Logger logger = LoggerFactory.getLogger(AsyncSender.class);

	private ChannelHolder channelHolder_ = null;

	private PendingManager pending_ = null;

	private long[] startTimes_ = new long[ClientConstants.NUM_MESSAGES];

	private CountDownLatch startSignal_;

	private ClientSPI clientSPI;

	public long[] getStartTimes() {
		return startTimes_;
	}

	public AsyncSender(String name, ChannelHolder channel,
			PendingManager pending, CountDownLatch startSignal,
			ClientSPI clientSPI) {
		this.setName(name);
		channelHolder_ = channel;
		pending_ = pending;
		startSignal_ = startSignal;

		this.clientSPI = clientSPI;
	}
	
    int numMessages = 0;
    
	public void run() {

		try {

			ByteBuffer sendBuffer = ByteBuffer
					.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

			logger.info(Thread.currentThread() + " started.");

			// Send the dummy messages.
			logger.debug(Thread.currentThread() + " Sending dummy requests.");

			for (int i = 0; i < ClientConstants.WARM_UP_NUM_MESSAGES; ++i) {
				// new message
				clientSPI.createNewMessage(i, sendBuffer);
				// send
				channelHolder_.sendMessage(sendBuffer);
			}

			logger.debug(Thread.currentThread()
					+ " Sending dummy requests - Done");

			// Wait for the reciever to recieve the dummy messages
			startSignal_.await();

			// Send the real requests.
			logger.debug(Thread.currentThread() + " Sending real requests.");

			for (int i = 0; i < ClientConstants.NUM_MESSAGES; ++i) {

				// new message
				clientSPI.createNewMessage(i, sendBuffer);

				// Wait if the pending requests exceed the maximum allowed.
				pending_.increase();

				startTimes_[i] = System.currentTimeMillis();
				// send
				channelHolder_.sendMessage(sendBuffer);

				numMessages++;
				
				if (logger.isDebugEnabled()) {
					logger.debug(getName() + " send:" + numMessages);
				}
				
			}

			logger.debug(Thread.currentThread() + " Sent "
					+ ClientConstants.NUM_MESSAGES + " messages.");

			logger.info(Thread.currentThread() + " finished.");

		} catch (Throwable ex) {
			logger.error("", ex);
		} finally {
		}
	}

}
