package com.shark.iopattern.touchstone.variable_length.competitors.shark;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.ProtocolSPI;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by weili5 on 5/20/16.
 */
public class AbstractServerWithWriter extends AbstractServer {

    BlockingQueue<BufferWriteChannelHolder> readableChannelQueue;

    LinkedBlockingQueue<BufferWriteChannelHolder> writeableChannelQueue;

    boolean synchronousWrite = false;
    WriteThread[] writeThreads_ = null;

    public AbstractServerWithWriter(ServerSPI serverSPI, ProtocolSPI protocolSPI) {
        super(serverSPI, protocolSPI);

        if (ServerConstants.WRITE_THREAD_POOL_SIZE <= 0) {
            synchronousWrite = true;

        } else {
            synchronousWrite = false;

            writeableChannelQueue = new LinkedBlockingQueue<BufferWriteChannelHolder>(
                    100);

            writeThreads_ = new WriteThread[ServerConstants.WRITE_THREAD_POOL_SIZE];

            for (int j = 0; j < writeThreads_.length; j++) {
                writeThreads_[j] = new WriteThread(getClass().getSimpleName() + "-WriteThread[" + j + "]");
            }

        }
    }

    @Override
    public void start() throws Exception {

        super.start();

        readableChannelQueue = new LinkedBlockingQueue<BufferWriteChannelHolder>(100);

        if (!synchronousWrite){
            // Start the write.
            for (int k = 0; k < writeThreads_.length; k++) {
                writeThreads_[k].start();
                runningThreads.add(writeThreads_[k]);
            }
        }
    }

    private class WriteThread extends Thread {

        WriteThread(String name) {
            super(name);
        }

        public void run() {
            try {

                logger.info(getName() + " started.");

                do {
                    BufferWriteChannelHolder channelHolder = writeableChannelQueue.take();
                    try {
                        channelHolder.flush();
                    } catch (IOException e) {
                        logger.warn("", e);
                        channelHolder.close();
                        continue;
                    }
                } while (started_);

            } catch (Exception ex) {
                logger.error("", ex);
            } finally{
                logger.info(this.getName() + ": stoped.");
            }
        }
    }

    protected void registerReadableChannel(BufferWriteChannelHolder proc) throws Exception {
        readableChannelQueue.put(proc);
    }

}
