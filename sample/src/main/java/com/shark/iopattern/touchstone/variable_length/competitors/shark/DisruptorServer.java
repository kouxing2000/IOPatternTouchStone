package com.shark.iopattern.touchstone.variable_length.competitors.shark;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.Constants;
import com.shark.iopattern.touchstone.share.ProtocolSPI;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TODO to be improved
 * Created by weili5 on 5/20/16.
 */
public class DisruptorServer extends AbstractServer {

    class HolderEvent {
        BufferWriteChannelHolder channelHolder;

        // init
        ByteBuffer sendBuffer = ByteBuffer
                .allocate(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

        ByteBuffer[] recvBuffers = new ByteBuffer[ServerConstants.BATCH_PROCESS_NUMBER];

        {
            for (int i = 0; i < recvBuffers.length; i++) {
                recvBuffers[i] = ByteBuffer
                        .allocate(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
            }
        }

        int batchSize = 0;
    }

    public DisruptorServer(ServerSPI serverSPI, ProtocolSPI protocolSPI) {
        super(serverSPI, protocolSPI);
    }

    Disruptor<HolderEvent> disruptor;
    RingBuffer<HolderEvent> ringBuffer;

    ExecutorService executorService;

    @Override
    public void start() throws Exception {
        super.start();

        // Construct the Disruptor
        executorService = Executors.newCachedThreadPool();
        disruptor = new Disruptor<HolderEvent>(new EventFactory<HolderEvent>(){
            @Override
            public HolderEvent newInstance() {
                return new HolderEvent();
            }
        }, 4096, executorService, ProducerType.MULTI, new BlockingWaitStrategy());

        // Connect the handler
        if (ServerConstants.READ_THREAD_POOL_SIZE > 0) {
            WorkHandler<HolderEvent>[] receiveHandlers = new WorkHandler[ServerConstants.READ_THREAD_POOL_SIZE];
            for (int i = 0; i < receiveHandlers.length; i++) {
                receiveHandlers[i] = new ReceiveWorkHandler();
            }
            WorkHandler<HolderEvent>[] processHandlers = new WorkHandler[ServerConstants.THREAD_POOL_SIZE];
            for (int i = 0; i < processHandlers.length; i++) {
                processHandlers[i] = new ProcessWorkHandler();
            }
            disruptor.handleEventsWithWorkerPool(receiveHandlers).thenHandleEventsWithWorkerPool(processHandlers);
        } else {
            WorkHandler<HolderEvent>[] handlers = new WorkHandler[ServerConstants.THREAD_POOL_SIZE];
            for (int i = 0; i < handlers.length; i++) {
                handlers[i] = new ReceiveProcessWorkHandler();
            }
            disruptor.handleEventsWithWorkerPool(handlers);
        }

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        ringBuffer = disruptor.getRingBuffer();

    }

    @Override
    public void stop() throws Exception {
        super.stop();

        disruptor.shutdown();
        executorService.shutdown();
    }

    @Override
    protected void registerReadableChannel(BufferWriteChannelHolder proc) throws Exception {
        long sequence = ringBuffer.next();  // Grab the next sequence
        try {
            HolderEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
            // for the sequence
            event.channelHolder = proc;
        } finally {
            ringBuffer.publish(sequence);
            //System.out.println("publish " + sequence + " " + proc);
        }
    }

    private class ReceiveProcessWorkHandler implements WorkHandler<HolderEvent> {
        private ProcessWorkHandler processWorkHandler = new ProcessWorkHandler();
        private ReceiveWorkHandler receiveWorkHandler = new ReceiveWorkHandler();
        @Override
        public void onEvent(HolderEvent holderEvent) throws Exception {
            receiveWorkHandler.onEvent(holderEvent);
            processWorkHandler.onEvent(holderEvent);
        }
    }

    private class ProcessWorkHandler implements WorkHandler<HolderEvent> {
        @Override
        public void onEvent(HolderEvent holderEvent) throws Exception {

            try {

                int batchSize = holderEvent.batchSize;
                ByteBuffer[] recvBuffers = holderEvent.recvBuffers;
                ByteBuffer sendBuffer = holderEvent.sendBuffer;

                for (int i = 0; i < batchSize; i++) {

                    ByteBuffer recvBuffer = recvBuffers[i];

                    try {
                        // decode
                        // new response
                        // encode
                        serverSPI.process(recvBuffer, sendBuffer);

                        // Send the response
                        holderEvent.channelHolder.sendMessage(sendBuffer);

                    } finally {
                        sendBuffer.clear();
                        //recvBuffer.clear();
                    }

                }

            } catch (Exception e) {
                logger.error("onEvent", e);
            }
        }
    }

    private class ReceiveWorkHandler implements WorkHandler<HolderEvent> {

        @Override
        public void onEvent(HolderEvent o) {

            try {

                int batchSize = 0;

                boolean channelFailed = false;

                BufferWriteChannelHolder channelHolder = o.channelHolder;
                ByteBuffer[] recvBuffers = o.recvBuffers;

                //System.out.println("process " + channelHolder);

                for (int i = 0; i < ServerConstants.BATCH_PROCESS_NUMBER; i++) {

                    ByteBuffer recvBuffer = recvBuffers[i];
                    recvBuffer.clear();

                    try {
                        // receive
                        if (!channelHolder.receiveMessage(recvBuffer)) {
                            logger.info("" + channelHolder + " read failed!");
                            channelFailed = true;
                            break;
                        } else {
                            batchSize++;
                        }
                    } catch (Exception e) {
                        logger.warn("", e);
                        channelFailed = true;
                        break;
                    }

                    if (i < recvBuffers.length - 1) {
                        if (!channelHolder.isStillReadable()) {
                            break;
                        }
                    }
                }

                if (!channelFailed) {
                    if (channelHolder.isStillReadable()) {
                        registerReadableChannel(channelHolder);
                    } else {
                        // need to register read key
                        channelHolder.beNotifiedWhenReadable();
                    }
                }

                o.batchSize = batchSize;

            } catch (Exception e) {
                logger.error("onEvent", e);
            }
        }
    }
}
