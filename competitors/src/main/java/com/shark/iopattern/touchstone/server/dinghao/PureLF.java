package com.shark.iopattern.touchstone.server.dinghao;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shark.iopattern.touchstone.server.ServerIF;
import com.shark.iopattern.touchstone.share.ProtocolSPI;

public class PureLF implements ServerIF {

    private static final Logger logger = LoggerFactory.getLogger(PureLF.class);

    private ServerSPI serverSPI;

    private ProtocolSPI protocolSPI;

    private Selector selector;

    private List<Thread> runningThreads = new ArrayList<Thread>();

    private ExecutorService asynWriteExecutor;

    private volatile boolean running = true;

    private ServerSocket socket;

    private final Object myLock = new Object();

    private boolean asynWrite = false;

    private boolean batchWrite = true;

    private boolean useQueue = true;

    public PureLF(ServerSPI serverSPI, ProtocolSPI protocolSPI) {
        this.serverSPI = serverSPI;
        this.protocolSPI = protocolSPI;
        headerSize = protocolSPI.headerLength();
        if (ServerConstants.WRITE_THREAD_POOL_SIZE > 0) {
            asynWrite = true;
        }
        if (!asynWrite) {
            if (ServerConstants.BATCH_PROCESS_NUMBER <= 0)
                batchWrite = false;
        }
        if (ServerConstants.QUEUE_SIZE <= 0) {
            useQueue = false;
        }
    }

    private ServerSocketChannel serverChannel;

    @Override
    public void start() throws Exception {
        running = true;
        selector = SelectorProvider.provider().openSelector();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        socket = serverChannel.socket();
        socket.setReceiveBufferSize(ServerConstants.SOCKET_RECV_BUFFER_SIZE);
        socket.bind(new InetSocketAddress(ServerConstants.SERVER_IP, ServerConstants.SERVER_PORT));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        for (int i = 0; i < ServerConstants.THREAD_POOL_SIZE; i++) {
            Thread worker = new Thread(new Worker());
            worker.start();
            runningThreads.add(worker);
        }
        if (asynWrite) {
            asynWriteExecutor = Executors.newFixedThreadPool(ServerConstants.WRITE_THREAD_POOL_SIZE);
            for (int i = 0; i < ServerConstants.WRITE_THREAD_POOL_SIZE; i++) {
                asynWriteExecutor.execute(new Writer());
            }
        }

    }

    @Override
    public void stop() throws Exception {
        running = false;
        socket.close();
        serverChannel.close();
        selector.close();
        for (Thread t : runningThreads) {
            if (t.isAlive()) {
                t.interrupt();
            }
        }
        for (Thread t : runningThreads) {
            if (t.isAlive()) {
                t.join();
            }
        }
        if (asynWrite) {
            asynWriteExecutor.shutdown();
        }
    }

    private int headerSize;

    private ArrayDeque<SelectionKey> taskQueue = new ArrayDeque<SelectionKey>(ServerConstants.THREAD_POOL_SIZE);

    private BlockingQueue<SelectionKey> writerQueue = new LinkedBlockingQueue<SelectionKey>();

    class ChannelAttachment {

        public ChannelAttachment() {
            readBuffer.limit(0);
        }

        final ByteBuffer readBuffer = ByteBuffer.allocateDirect(ServerConstants.READ_BUFFER_SIZE
                * ServerConstants.THREAD_POOL_SIZE + Constants.MESSAGE_BUFFER_INITIAL_CAPACITY); // read buffer

        // write buffer for asyn writer
        final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(ServerConstants.WRITE_BUFFER_SIZE);

        final Object readLock = new Object(); // read lock

        final Object writeLock = new Object(); // write lock

        private volatile boolean isClosed = false; // close the channel when isClosed == true and threadsNo == 0

        private AtomicInteger threadsNo = new AtomicInteger();
    }

    private void sendMsg(ByteBuffer sendBuffer, SocketChannel socketChannel, Object writeLock, ByteBuffer sendBuffers)
            throws IOException {
        synchronized (writeLock) {
            if (sendBuffers.remaining() < sendBuffer.remaining()) {
                flush(socketChannel, sendBuffers);
            }
            sendBuffers.put(sendBuffer);
        }
    }

    private void flush(SocketChannel socketChannel, ByteBuffer sendBuffers) throws IOException {
        sendBuffers.flip();
        while (sendBuffers.remaining() > 0) {
            socketChannel.write(sendBuffers);
        }
        sendBuffers.clear();
    }

    class Writer implements Runnable {

        @Override
        public void run() {
            while (running) {
                try {
                    SelectionKey key = writerQueue.take();
                    SocketChannel socketChannel = (SocketChannel) key.channel();
                    ChannelAttachment attach = (ChannelAttachment) key.attachment();
                    synchronized (attach.writeLock) {
                        flush(socketChannel, attach.writeBuffer);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    class Worker implements Runnable {

        @Override
        public void run() {
            while (running) {
                SelectionKey readKey = null;
                Set<SelectionKey> keys = selector.selectedKeys();
                boolean processOnly = false;

                synchronized (myLock) {
                    if (useQueue) {
                        while (readKey == null && keys.isEmpty()) {
                            try {
                                int ready = selector.selectNow();
                                if (ready == 0) { // nothing return
                                    readKey = taskQueue.peek();
                                    if (readKey == null) {
                                        selector.select();
                                    } else {
                                        processOnly = true;
                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            keys = selector.selectedKeys();
                        }
                    } else {
                        while (keys.isEmpty()) {
                            try {
                                selector.select();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            keys = selector.selectedKeys();
                        }
                    }

                    if (!processOnly) {
                        Iterator<SelectionKey> selectedKeys = keys.iterator();
                        while (selectedKeys.hasNext()) {
                            SelectionKey key = selectedKeys.next();
                            selectedKeys.remove();
                            if (key.isValid()) {
                                if (key.isReadable()) {
                                    if (logger.isDebugEnabled())
                                        logger.debug("read data...");
                                    ChannelAttachment attach = (ChannelAttachment) key.attachment();
                                    if (attach == null) {
                                        attach = new ChannelAttachment();
                                        key.attach(attach);
                                    }
                                    readKey = key;
                                    if (useQueue)
                                        taskQueue.offer(readKey);
                                } else if (key.isAcceptable()) {
                                    if (logger.isDebugEnabled())
                                        logger.debug("accept data...");
                                    accept(key);
                                }
                                break;
                            }
                        }
                    }
                }

                selector.wakeup();
                if (readKey != null) {
                    ChannelAttachment attach = (ChannelAttachment) readKey.attachment();
                    try {
                        attach.threadsNo.incrementAndGet();
                        if (processOnly) {
                            process(readKey);
                        } else {
                            readData(readKey);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        readKey.cancel();
                    } finally {
                        attach.threadsNo.decrementAndGet();
                        if (attach.isClosed && attach.threadsNo.get() == 0) {
                            SocketChannel socketChannel = (SocketChannel) readKey.channel();
                            try {
                                readKey.cancel();
                                socketChannel.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (!processOnly && useQueue) {
                            synchronized (myLock) {
                                taskQueue.remove(readKey);
                            }
                        }
                    }
                }
            }
        }

        private boolean readMessage(ChannelAttachment attach, ByteBuffer recvBuffer) {
            ByteBuffer readBuffer = attach.readBuffer;
            boolean hasMsg = false;
            synchronized (attach.readLock) {
                int pos = readBuffer.position();
                int limit = readBuffer.limit();
                if (limit > pos + headerSize) {
                    int packageLength = protocolSPI.packageLength(readBuffer);
                    readBuffer.position(pos);
                    if (limit >= pos + packageLength) {
                        readBuffer.limit(pos + packageLength);
                        recvBuffer.put(readBuffer);
                        readBuffer.limit(limit);
                        hasMsg = true;
                    }
                }
            }
            return hasMsg;
        }

        // Buffer for response.
        private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

        // Buffer for request.
        private final ByteBuffer recvBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

        private void process(SelectionKey key) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            ChannelAttachment attach = (ChannelAttachment) key.attachment();
            while (readMessage(attach, recvBuffer)) {
                sendBuffer.clear();
                recvBuffer.flip();
                serverSPI.process(recvBuffer, sendBuffer);
                recvBuffer.clear();
                if (asynWrite) {
                    ByteBuffer writeBuffers = attach.writeBuffer;
                    synchronized (attach.writeLock) {
                        if (writeBuffers.position() == 0) {
                            writerQueue.offer(key);
                        }
                        writeBuffers.put(sendBuffer);
                    }
                } else {
                    if (batchWrite) {
                        sendMsg(sendBuffer, socketChannel, attach.writeLock, attach.writeBuffer);
                    } else {
                        while (sendBuffer.remaining() > 0)
                            socketChannel.write(sendBuffer);
                    }
                }
            }

            if (!asynWrite && batchWrite) {
                synchronized (attach.writeLock) {
                    flush(socketChannel, attach.writeBuffer);
                }
            }
        }

        private void readData(SelectionKey key) throws IOException {

            SocketChannel socketChannel = (SocketChannel) key.channel();
            ChannelAttachment attach = (ChannelAttachment) key.attachment();
            ByteBuffer buffer = attach.readBuffer;
            int numOfBytes = -1;

            synchronized (attach.readLock) {
                buffer.mark();
                buffer.position(buffer.limit());
                buffer.limit(buffer.limit() + ServerConstants.READ_BUFFER_SIZE);
                numOfBytes = socketChannel.read(buffer);
                buffer.limit(buffer.position());
                buffer.reset();
            }

            process(key);
            synchronized (attach.readLock) {
                buffer.compact();
                buffer.flip();
            }

            if (numOfBytes == -1) {
                if (asynWrite) {
                    synchronized (attach.writeLock) {
                        flush(socketChannel, attach.writeBuffer);
                    }
                }
                attach.isClosed = true;
            }

        }

        private void accept(SelectionKey key) {
            // For an accept to be pending the channel must be a server socket
            // channel.
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            SocketChannel socketChannel = null;
            try {
                // Accept the connection and make it non-blocking
                socketChannel = serverSocketChannel.accept();
                if (socketChannel == null)
                    return;
                socketChannel.configureBlocking(false);
                Socket socket = socketChannel.socket();
                if (ServerConstants.TCP_NODELAY) {
                    socket.setTcpNoDelay(true);
                    logger.info("DiameterServer TCP_NODELAY is ON");
                }
                logger.info("socket recv buffer size is set to " + socket.getReceiveBufferSize() + " bytes");
                socket.setSendBufferSize(ServerConstants.SOCKET_SEND_BUFFER_SIZE);
                socket.setReceiveBufferSize(ServerConstants.SOCKET_RECV_BUFFER_SIZE);
                logger.info("socket send buffer size is set to " + socket.getSendBufferSize() + " bytes");
                socketChannel.register(selector, SelectionKey.OP_READ);
            } catch (IOException e) {
                e.printStackTrace();
                key.cancel();
            }

        }

    }

}
