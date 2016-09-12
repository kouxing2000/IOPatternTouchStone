/**
 * 
 */
package com.shark.iopattern.touchstone.variable_length.competitors.shark;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.shark.iopattern.touchstone.server.ServerIF;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.share.ChannelHolder;
import com.shark.iopattern.touchstone.share.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shark.iopattern.touchstone.share.ProtocolSPI;


/**
 * @author weili1
 * 
 */
public class IUMLFServer implements ServerIF {
    volatile boolean started_ = false;

    final Logger logger = LoggerFactory.getLogger(getClass());

    private ServerSocketChannel serverChannel;

    private Thread bossThread;

    ServerSPI serverSPI;

    ProtocolSPI protocolSPI;

    Selector selector;

    List<Thread> runningThreads = new ArrayList<Thread>();

    public IUMLFServer(ServerSPI serverSPI, ProtocolSPI protocolSPI) {

        this.serverSPI = serverSPI;
        this.protocolSPI = protocolSPI;

    }

    public synchronized void start() throws Exception {

        if (started_) {
            throw new IllegalStateException("Pool already started.");
        }

        started_ = true;

        serverChannel = ServerSocketChannel.open();
        ServerSocket socket = serverChannel.socket();
        socket.setReceiveBufferSize(ServerConstants.SOCKET_RECV_BUFFER_SIZE);

        socket.bind(new InetSocketAddress(ServerConstants.SERVER_IP, ServerConstants.SERVER_PORT));

        // set non-blocking mode for the listening socket
        serverChannel.configureBlocking(ServerConstants.BLOCKING_MODE);

        // create a new Selector for use below
        selector = Selector.open();

        // register the ServerSocketChannel with the Selector
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        bossThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        // this may block for a long time, upon return the
                        // selected set contains keys of the ready channels
                        int n = selector.select();

                        if (n == 0) {
                            continue; // nothing to do
                        }

                        // get an iterator over the set of selected keys
                        Iterator it = selector.selectedKeys().iterator();

                        // look at each key in the selected set
                        while (it.hasNext()) {
                            SelectionKey key = (SelectionKey) it.next();

                            // Is a new connection coming in?
                            if (key.isAcceptable()) {
                                ServerSocketChannel server = (ServerSocketChannel) key.channel();
                                SocketChannel channel = server.accept();

                                registerChannel(selector, channel, key);
                            }

                            // remove key from selected set, it's been handled
                            it.remove();

                        }
                    }
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        };

        bossThread.start();

    }

    public synchronized void stop() throws Exception {
        started_ = false;

        selector.close();
        bossThread.interrupt();
        serverChannel.close();

        for (Thread t : runningThreads) {
            if (t.isAlive()) {
                System.out.println("interrupt thread:" + t);
                t.interrupt();
            }
        }
        
        runningThreads.clear();
    }

    private class LFThread extends Thread {

        ChannelHolder readableChannel;
        ReentrantLock lock;
        LFThread(String name, int id, ChannelHolder channelHodler, ReentrantLock lock) {
            super(name);
            this.readableChannel = channelHodler;
            this.lock = lock;
        }

        public void run() {
            try {
                logger.info(this.getName() + ": started.");
                
                runningThreads.add(this);
                
                // init
                ByteBuffer sendBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

                ByteBuffer[] recvBuffers = new ByteBuffer[ServerConstants.BATCH_PROCESS_NUMBER];

                for (int i = 0; i < recvBuffers.length; i++) {
                    recvBuffers[i] = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
                }

                do {

                    int batchSize = 0;

                    boolean channelFailed = false;

                    ChannelHolder channelHolder = readableChannel;
                    
                    lock.lock();
                    
                    try{

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
                            logger.warn(Thread.currentThread().toString(), e);
                            channelFailed = true;
                            break;
                        }

                        if (i < recvBuffers.length - 1) {
                            if (!channelHolder.isStillReadable()) {
                                break;
                            }
                        }
                    }
                    
                    }finally{
                        lock.unlock();
                    }
                    
                    for (int i = 0; i < batchSize; i++) {

                        ByteBuffer recvBuffer = recvBuffers[i];

                        try {
                            // decode
                            // new response
                            // encode
                            serverSPI.process(recvBuffer, sendBuffer);

                            // Send the response
                            channelHolder.sendMessage(sendBuffer);

                        } finally {
                            sendBuffer.clear();
                            // recvBuffer.clear();
                        }

                    }

                    if (channelFailed) {
                        break;
                    }

                } while (started_);
            } catch (Exception ex) {
               logger.info(Thread.currentThread().toString(), ex);
            } finally {
                logger.info(this.getName() + ": stoped.");
            }

        }
    }

    /**
     * Register the given channel with the given selector for the given operations of interest
     */
    protected void registerChannel(Selector selector, SocketChannel channel, SelectionKey key) throws Exception {
        if (channel == null) {
            return; // could happen
        }

        logger.info("New Channel:" + channel);

        Socket socket = channel.socket();
        channel.configureBlocking(ServerConstants.BLOCKING_MODE);
        if (ServerConstants.TCP_NODELAY) {
            socket.setTcpNoDelay(true);
            logger.info(" TCP_NODELAY is ON");
        }
        logger.info("socket recv buffer size is set to " + socket.getReceiveBufferSize() + " bytes");
        socket.setSendBufferSize(ServerConstants.SOCKET_SEND_BUFFER_SIZE);
        logger.info("socket send buffer size is set to " + socket.getSendBufferSize() + " bytes");

        // new ChannelHolder
        ChannelHolder channelHodler = createChannelHolder(channel, key);

        key.attach(channelHodler);
        
        ReentrantLock lock = new ReentrantLock();

        LFThread[] threads = new LFThread[ServerConstants.THREAD_POOL_SIZE];
        
        for (int i = 0; i < threads.length; i++) {
            String name = getClass().getSimpleName() + "-LFThread[" + channel + "-" + i + "]";
            threads[i] = new LFThread(name, i, channelHodler, lock);
            threads[i].start();
        }
        
    }

    protected ChannelHolder createChannelHolder(SocketChannel channel, SelectionKey key) {
        return new SelectionKeyChannelHolder(key, channel, protocolSPI);
    }

}
