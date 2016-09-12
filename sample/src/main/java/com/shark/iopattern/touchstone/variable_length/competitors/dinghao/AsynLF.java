package com.shark.iopattern.touchstone.variable_length.competitors.dinghao;

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
import java.util.concurrent.atomic.AtomicInteger;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerIF;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shark.iopattern.touchstone.share.ProtocolSPI;

public class AsynLF implements ServerIF {

    private static final Logger logger = LoggerFactory.getLogger(AsynLF.class);

    private ServerSPI serverSPI;

    private ProtocolSPI protocolSPI;

    private Selector selector;

    private List<Thread> runningThreads = new ArrayList<Thread>();

    private volatile boolean running = true;

    private ServerSocketChannel serverChannel;

    private ServerSocket socket;

    private final Object myLock = new Object();

    private int headerSize;

    private ArrayDeque<SelectionKey> readQueue = new ArrayDeque<SelectionKey>(ServerConstants.THREAD_POOL_SIZE);

    private boolean useQueue = true;

    public AsynLF(ServerSPI serverSPI, ProtocolSPI protocolSPI) {
        this.serverSPI = serverSPI;
        this.protocolSPI = protocolSPI;
        headerSize = protocolSPI.headerLength();
        if (ServerConstants.QUEUE_SIZE <= 0) {
            useQueue = false;
        }
    }

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
            Thread worker = new Thread(new AsynWorker());
            worker.start();
            runningThreads.add(worker);
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
    }

    class ChannelAttachment {

        public ChannelAttachment() {
            readBuffer.limit(0);
        }

        final ByteBuffer readBuffer = ByteBuffer.allocateDirect(ServerConstants.READ_BUFFER_SIZE
                * ServerConstants.THREAD_POOL_SIZE + Constants.MESSAGE_BUFFER_INITIAL_CAPACITY); // read buffer

        final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(ServerConstants.WRITE_BUFFER_SIZE); // write buffer

        final Object readLock = new Object(); // read lock

        final Object writeLock = new Object(); // write lock

        private boolean writable = false;

        private volatile boolean isClosed = false; // close the channel when isClosed == true and threadsNo == 0

        private AtomicInteger threadsNo = new AtomicInteger();
    }

    class AsynWorker implements Runnable {

        // Buffer for response.
        private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

        // Buffer for request.
        private final ByteBuffer recvBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

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
                    logger.info(" TCP_NODELAY is ON");
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

        private void process(SelectionKey key) throws IOException {
            ChannelAttachment attach = (ChannelAttachment) key.attachment();
            while (readMessage(attach, recvBuffer)) {
                sendBuffer.clear();
                recvBuffer.flip();
                serverSPI.process(recvBuffer, sendBuffer);
                recvBuffer.clear();
                ByteBuffer writeBuffers = attach.writeBuffer;
                synchronized (attach.writeLock) {
                    if (writeBuffers.remaining() < sendBuffer.remaining()) {
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        writeBuffers.flip();
                        while (writeBuffers.remaining() > 0)
                            socketChannel.write(writeBuffers);
                        writeBuffers.clear();
                    }
                    writeBuffers.put(sendBuffer);
                    if (!attach.writable) {
                        attach.writable = true;
                        // be able to write again
                        int interestOps = key.interestOps();
                        interestOps |= SelectionKey.OP_WRITE;
                        key.interestOps(interestOps);
                    }
                }
            }

            write(key);

        }

        private void read(SelectionKey key) throws IOException {
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
                attach.isClosed = true;
            }

        }

        private void write(SelectionKey key) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            ChannelAttachment attach = (ChannelAttachment) key.attachment();
            ByteBuffer sendBuffers = attach.writeBuffer;
            synchronized (attach.writeLock) {
                sendBuffers.flip();
                while (sendBuffers.remaining() > 0)
                    socketChannel.write(sendBuffers);
                sendBuffers.clear();
                // remove the interest of write
                if (attach.writable) {
                    attach.writable = false;
                    int interestOps = key.interestOps();
                    interestOps &= ~SelectionKey.OP_WRITE;
                    key.interestOps(interestOps);
                }
            }
        }

        @Override
        public void run() {
            while (running) {
                SelectionKey readyKey = null;
                Set<SelectionKey> keys = selector.selectedKeys();
                int state = 1; // 0:process;1:read;2:write
                synchronized (myLock) {
                    if (useQueue) {
                        while (readyKey == null && keys.isEmpty()) {
                            try {
                                int ready = selector.selectNow();
                                if (ready == 0) { // nothing return
                                    readyKey = readQueue.peek();
                                    if (readyKey == null) {
                                        selector.select();
                                    } else {
                                        state = 0;
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

                    if (state > 0) {
                        Iterator<SelectionKey> selectedKeys = keys.iterator();

                        while (selectedKeys.hasNext()) {
                            SelectionKey key = selectedKeys.next();
                            selectedKeys.remove();
                            if (key.isValid()) {
                                if (key.isAcceptable()) {
                                    accept(key);
                                } else if (key.isWritable()) {
                                    readyKey = key;
                                    state = 2;
                                } else if (key.isReadable()) {
                                    ChannelAttachment attach = (ChannelAttachment) key.attachment();
                                    if (attach == null) {
                                        attach = new ChannelAttachment();
                                        key.attach(attach);
                                    }
                                    readyKey = key;
                                    state = 1;
                                    if (useQueue)
                                        readQueue.offer(readyKey);
                                }
                                break;
                            }
                        }
                    }

                }

                selector.wakeup();
                if (readyKey != null) {
                    ChannelAttachment attach = (ChannelAttachment) readyKey.attachment();
                    try {
                        attach.threadsNo.incrementAndGet();
                        switch (state) {
                        case 0:
                            process(readyKey);
                            break;
                        case 1:
                            read(readyKey);
                            break;
                        case 2:
                            write(readyKey);
                            break;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        readyKey.cancel();
                    } finally {
                        if (attach.isClosed && attach.threadsNo.decrementAndGet() == 0) {
                            SocketChannel socketChannel = (SocketChannel) readyKey.channel();
                            try {
                                readyKey.cancel();
                                socketChannel.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (state == 1 && useQueue) {
                            synchronized (myLock) {
                                readQueue.remove(readyKey);
                            }
                        }
                    }
                }
            }
        }

    }
}
