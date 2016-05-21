package com.shark.iopattern.touchstone.client;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import com.shark.iopattern.touchstone.share.BytesHexDumper;
import com.shark.iopattern.touchstone.share.ChannelHolder;
import com.shark.iopattern.touchstone.share.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shark.iopattern.touchstone.share.ProtocolSPI;


public class AsyncReciever extends Thread {
	
	private Logger logger = LoggerFactory.getLogger(AsyncReciever.class);

    private ChannelHolder channelHolder_ = null;

    private long[] endTimes_;
    
    private long[] startTimes_;

    public void setStartTimes(long[] startTimes_) {
		this.startTimes_ = startTimes_;
	}

    private PendingManager pending_ = null;
    
    private  CountDownLatch doneSignal_;
    
    private CountDownLatch startSignal_;
    
    private ProtocolSPI protocolSPI;
    
    private ResultConclude resultConclude;
    
    private ClientSPI clientSPI;

    public AsyncReciever(String name, ChannelHolder channel, ResultConclude resultConclude,
    		PendingManager pending, CountDownLatch startSignal,  CountDownLatch doneSignal, ProtocolSPI protocolSPI, ClientSPI clientSPI) {
        setName(name);
    	channelHolder_ = channel;
        pending_ = pending;
        
        startSignal_ = startSignal ;
        doneSignal_ =  doneSignal;
        
        this.protocolSPI = protocolSPI;
        this.resultConclude = resultConclude;
        this.clientSPI = clientSPI;
        
        endTimes_ = new long[ClientConstants.NUM_MESSAGES];
        for (int i = 0; i < endTimes_.length; i++) {
        	endTimes_[i] = -1;
		}
    }
    
    long numMessages = 0;

    public void run() {
        ByteBuffer recvBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
        
        try {
            logger.info(Thread.currentThread() +  " started.");

            // Recieve the dummy requests.
            logger.debug(Thread.currentThread() + " Recieving dummy responses.");
            for (int i = 0; i < ClientConstants.WARM_UP_NUM_MESSAGES; ++i) {
                recvBuffer.clear();
                
                // receive
              if (!channelHolder_.receiveMessage(recvBuffer)){
                	logger.info(Thread.currentThread() + " Failed to read message");
                	return;
                }  
            }
            
            logger.debug(Thread.currentThread() + " Recieving dummy responses - Done.");

            // Start sending the real requests.
            
            startSignal_.countDown();
            
            startSignal_.await();

            // Recieve and record the latency.
            logger.debug(Thread.currentThread() + " Recieving real responses.");

           long startTime = System.currentTimeMillis();
           
            while (true) {
                recvBuffer.clear();
                
                // receive
                if (!channelHolder_.receiveMessage(recvBuffer)){
                	logger.warn(Thread.currentThread() + " Failed to read message");
                	return;
                }  
                
                if (!clientSPI.validateAnswer(recvBuffer)){
                	logger.error("Answer not correct! receive buffer dump:\n" + BytesHexDumper.getHexdump(recvBuffer));
                	return;
                }
                
                long endTime = System.currentTimeMillis();
                pending_.decrease();

                //judge which request
                int msgIndex = protocolSPI.getRequestID(recvBuffer);
                
                if (endTimes_[msgIndex] < 0){
                	endTimes_[msgIndex] = endTime;
                }else{
                	logger.error("duplicate response? " + msgIndex);
                }
               
                numMessages++;
                
                if (logger.isDebugEnabled()){
                	logger.debug(getName() + "-" +  channelHolder_ + " Rece:" + numMessages);
                }
                
                if (numMessages >= ClientConstants.NUM_MESSAGES) {
                    doneSignal_.countDown();

                    long stopTime = System.currentTimeMillis();

                	logger.info(Thread.currentThread()  + " finished. \nTake time:" + (stopTime - startTime) + " ms!");
                    
                    long[] startTimes = startTimes_;
                    for (int i = 0; i < ClientConstants.NUM_MESSAGES; ++i) {
                    	if (endTimes_[i] < 0){
                    		logger.error("no response? " + i);
                    	}
                        endTimes_[i] = (endTimes_[i] - startTimes[i]);
                    }
                    
                    resultConclude.add(endTimes_, numMessages);
                    
                    return;
                }
            }
        } catch (Throwable ex) {
           logger.error("", ex);
        }finally{
        	channelHolder_.close();
        }
            
    }

}