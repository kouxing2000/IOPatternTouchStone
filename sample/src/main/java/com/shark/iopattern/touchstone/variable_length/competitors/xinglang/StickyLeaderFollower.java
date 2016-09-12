package com.shark.iopattern.touchstone.variable_length.competitors.xinglang;

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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.share.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shark.iopattern.touchstone.server.ServerIF;
import com.shark.iopattern.touchstone.share.ProtocolSPI;

/**
 * A leader follow prototype implementation.
 * 
 * The configuration guideline:
 * 1. Choose write strategy by setting the BATCH_PROCESS_NUMBER
 *    0 - OPTIMIZED_SYNC_WRITE
 *    1 - SERIAL_WRITE
 *    2 - BULK_WRITE
 *    3 - SYNC_WRITE
 * 2. Choose read buffer size and write buffer size, the write buffer size should
 * be set carefully if the client have control the traffic.
 * 
 * @author Xinglang Wang
 */
public class StickyLeaderFollower implements ServerIF {
    // Logger
    private static final Logger logger = LoggerFactory.getLogger(StickyLeaderFollower.class);

    private static enum WriteStrategy {
        OPTIMIZED_SYNC_WRITE, SERIAL_WRITE, BULK_WRITE, SYNC_WRITE
    }

    // A volatile flag to be used for gracefully shutdown
    private static volatile boolean running;

    // The server channel
    private ServerSocketChannel serverChannel;

    // The main selector we'll be monitoring
    private Selector selector;

    // SPI instance for server logic
    static ServerSPI serverSPI;

    // SPI instance for handling protocol specific logic
    static ProtocolSPI protocolSPI;

    // Thread pool
    private List<Thread> runningThreads = new ArrayList<Thread>();

    // Construct
    public StickyLeaderFollower(ServerSPI serverSPIX, ProtocolSPI protocolSPIX) {
        serverSPI = serverSPIX;
        protocolSPI = protocolSPIX;
    }

    /**
     * Start the thread pool.
     */
    @Override
    public void start() throws Exception {
        selector = initSelector();
        running = true;

        WriteStrategy writeStrategy;

        switch (ServerConstants.BATCH_PROCESS_NUMBER) {
        case 0:
            writeStrategy = WriteStrategy.OPTIMIZED_SYNC_WRITE;
            break;
        case 1:
            writeStrategy = WriteStrategy.SERIAL_WRITE;
            break;
        case 2:
            writeStrategy = WriteStrategy.BULK_WRITE;
            break;
        default:
            writeStrategy = WriteStrategy.SYNC_WRITE;

        }
        int poolSize = Math.max(Runtime.getRuntime().availableProcessors(), ServerConstants.THREAD_POOL_SIZE);
        for (int i = 0; i < poolSize; i++) {
            Worker t = new Worker(selector, serverSPI, writeStrategy, poolSize);
            t.start();
            runningThreads.add(t);
        }
    }

    /**
     * Stop the thread pool.
     */
    @Override
    public void stop() throws Exception {
        running = false;
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
        try {
            selector.close();
            serverChannel.close();
        } catch (Exception e) {
        }
    }

    /**
     * Initialize main selector.
     * 
     * @return
     * @throws IOException
     */
    private Selector initSelector() throws IOException {
        Selector socketSelector = SelectorProvider.provider().openSelector();
        this.serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        ServerSocket socket = serverChannel.socket();
        socket.setReceiveBufferSize(ServerConstants.SOCKET_RECV_BUFFER_SIZE);
        socket.bind(new InetSocketAddress(ServerConstants.SERVER_IP, ServerConstants.SERVER_PORT));
        serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
        return socketSelector;
    }

    /**
     * A data structure to store channel info. It will be used as attachment of the selection key.
     */
    private final static class ChannelBuffer {
        /** The buffer to read bulk messages from socket */
        private final ByteBuffer readBuffer;

        /** The buffer to write bulk messages to socket */
        private ByteBuffer writeInputBuffer;

        /** The buffer to write bulk messages to socket */
        private ByteBuffer writeOutputBuffer;

        /** Guard read buffer */
        private final Lock readLock = new ReentrantLock();

        /** Guard writeInputQueue */
        private final Lock writeLock = new ReentrantLock();

        /** Guard socket write */
        private final Lock flushLock = new ReentrantLock();

        /** The channel selection key of this buffer. */
        private final SelectionKey key;

        // A flag to determine dirty channel buffer.
        private int readSequence = 0;

        // A flag to ensure flush and avoid write lock contention.
        private int writeSequence = 0;
        
        // Statistics 
        private int readCount = 0;
        
        private int readTotal = 0;

        private int writeCount = 0;
        
        private int writeTotal = 0;

        /** Header size of the protocol */
        final int headerSize;

        /** Current read buffer length */
        int bufferedLength;

        /**
         * Create a channel buffer and initialize the byte buffers.
         * 
         * @param key
         *            SelectionKey of the channel
         * @param headerSize
         *            Protocol header size
         */
        public ChannelBuffer(SelectionKey key, int headerSize) {
            logger.info("Create a channelBuffer");
            readBuffer = ByteBuffer.allocateDirect(ServerConstants.READ_BUFFER_SIZE
                    + Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);
            writeInputBuffer = ByteBuffer.allocateDirect(ServerConstants.WRITE_BUFFER_SIZE);
            writeOutputBuffer = ByteBuffer.allocateDirect(ServerConstants.WRITE_BUFFER_SIZE);
            this.key = key;
            this.headerSize = headerSize;
            logger.info("Read buffer size:" + readBuffer.capacity());
            logger.info("Write buffer size:" + writeInputBuffer.capacity());
        }

        /**
         * Read message from socket to read buffer.
         * 
         * @return false if there is no available data in the socket.
         */
        private boolean initialize() {
            readCount++;
            SocketChannel socketChannel = (SocketChannel) key.channel();
            try {
                int numOfBytes;
                if ((numOfBytes = socketChannel.read(readBuffer)) <= 0) {
                    if (numOfBytes == -1) {
                        cancelChannel(socketChannel);
                    }
                    return false;
                }
                readTotal += numOfBytes;
            } catch (IOException e) {
                logger.error("Fail to read message from socket", e);
                cancelChannel(socketChannel);
                return false;
            }
            readBuffer.flip();
            this.bufferedLength = readBuffer.limit();
            return true;
        }

        /**
         * Read a message from the read buffer.
         * 
         * @param recvBuffer
         * @param sequence
         *            sequence when the thread take the channel
         * @return -1 means no message, 0 means no additional message, 1 means has more message.
         */
        public int readMessage(ByteBuffer recvBuffer, int sequence) {
            readLock.lock();
            try {
                if (readSequence != sequence) {
                    return -1;
                }
                if (bufferedLength == 0 && !initialize()) {
                    readSequence++;
                    return -1;
                }
                int retCode;
                if ((retCode = copyMessage(recvBuffer)) <= 0) {
                    readSequence++;
                    bufferedLength = 0;
                }
                return retCode;
            } finally {
                readLock.unlock();
            }
        }

        /**
         * Copy message to thread specific small buffer.
         * 
         * @param recvBuffer
         * @return -1 means no message, 0 means no additional message, 1 means has more message.
         */
        private int copyMessage(ByteBuffer recvBuffer) {
            int position = readBuffer.position();
            if (bufferedLength - position >= headerSize) {
                int nextPosition = position + protocolSPI.packageLength(readBuffer);
                readBuffer.position(position);
                if (bufferedLength > nextPosition) {
                    readBuffer.limit(nextPosition);
                    recvBuffer.put(readBuffer);
                    readBuffer.limit(bufferedLength);
                    return 1;
                } else if (bufferedLength == nextPosition) {
                    recvBuffer.put(readBuffer);
                    readBuffer.clear();
                    return 0;
                }
            }
            readBuffer.compact();
            return -1;
        }

        /**
         * Double check sync write message to socket.
         * 
         * @param sendBuffer
         */
        public void optimizedSyncWrite(ByteBuffer sendBuffer) {
            if (flushLock.tryLock()) {
                // If the thread can get flush lock, do flush.
                optimizedFlush(sendBuffer);
            } else {
                // otherwise get writelock
                writeLock.lock();
                // double check flushLock to make sure sync write.
                if (flushLock.tryLock()) {
                    writeLock.unlock();
                    optimizedFlush(sendBuffer);
                } else {
                    // Otherwise append it to channel buffer.
                    if (writeInputBuffer.remaining() > sendBuffer.remaining()) {
                        writeInputBuffer.put(sendBuffer);
                        writeLock.unlock();
                    } else {
                        writeLock.unlock();
                        flushLock.lock();
                        optimizedFlush(sendBuffer);
                    }
                }
            }
        }

        /**
         * Flush data to socket.
         * 
         * @param sendBuffer
         */
        private void optimizedFlush(ByteBuffer sendBuffer) {
            boolean hasWriteLock = false;
            try {
                SocketChannel socketChannel = (SocketChannel) key.channel();
    
                writeToSocket(socketChannel, sendBuffer);
                while (true) {
                    writeLock.lock();
                    hasWriteLock = true;
                    if (writeInputBuffer.position() == 0) {
                        // Will hold writelock before release flush lock to avoid
                        // losing response.
                        break;
                    } else {
                        exchangeOutputBuffer();
                        writeLock.unlock();
                        hasWriteLock = false;
                    }
                    flushOutputBuffer();
                }
            } finally {
                flushLock.unlock();
                if (hasWriteLock) {
                    writeLock.unlock();
                }
            }
        }

        private void exchangeOutputBuffer() {
            ByteBuffer t = writeOutputBuffer;
            writeOutputBuffer = writeInputBuffer;
            writeInputBuffer = t;
        }

        /**
         * Serial write with write buffer.
         * 
         * @param sendBuffer
         * @return
         */
        public int serialWrite(ByteBuffer sendBuffer) {
            writeLock.lock();
            int c = ++writeSequence;
            if (writeInputBuffer.remaining() >= sendBuffer.remaining()) {
                writeInputBuffer.put(sendBuffer);
                writeLock.unlock();
            } else {
                flushLock.lock();
                exchangeOutputBuffer();
                writeInputBuffer.put(sendBuffer);
                writeLock.unlock();
                try {
                    flushOutputBuffer();
                } finally {
                    flushLock.unlock();
                }
            }
            return c;
        }

        /**
         * Ensure flush for serial write strategy.
         * 
         * @param writeSequence
         * @return
         */
        public boolean ensureFlush(int writeSequence) {
            if (!writeLock.tryLock()) {
                return false;
            }

            if (this.writeSequence != writeSequence || writeInputBuffer.position() == 0) {
                writeLock.unlock();
                return true;
            }
            flushLock.lock();
            exchangeOutputBuffer();
            writeLock.unlock();
            try {
                flushOutputBuffer();
            } finally {
                flushLock.unlock();
            }
            return true;
        }

        /**
         * Bulk write with thread local buffer.
         * 
         * @param localCache
         * @param sendBuffer
         */
        public void bulkWrite(ByteBuffer localCache, ByteBuffer sendBuffer) {
            if (localCache.remaining() >= sendBuffer.remaining()) {
                localCache.put(sendBuffer);
            } else {
                localCache.flip();
                SocketChannel socketChannel = (SocketChannel) key.channel();
                writeLock.lock();
                try {
                    writeToSocket(socketChannel, localCache);
                } finally {
                    writeLock.unlock();
                }
                localCache.clear();
                localCache.put(sendBuffer);
            }
        }
        
        /**
         * Ensure flush for bulk write strategy.
         * 
         * @param localCache
         */
        public boolean ensureFlush(ByteBuffer localCache, boolean forced) {
            if (localCache.position() > 0) {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                if (forced) {
                    writeLock.lock();
                } else if (!writeLock.tryLock()) {
                    return false;
                }
                try {
                    localCache.flip();
                    writeToSocket(socketChannel, localCache);
                } finally {
                    writeLock.unlock();
                }
                localCache.clear();
                return true;
            }
            return true;
        }

        private void writeToSocket(SocketChannel socketChannel, ByteBuffer buffer) {
            writeTotal += buffer.remaining();
            try {
                while (buffer.hasRemaining()) {
                    writeCount++;
                    socketChannel.write(buffer);
                }
            } catch (IOException e) {
                logger.error("Fail to write message to socket", e);
                cancelChannel(socketChannel);
            }
        }

        /**
         * Sync write message to socket.
         * 
         * @param localCache
         */
        public void syncWrite(ByteBuffer buffer) {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            writeLock.lock();
            try {
                writeTotal += buffer.remaining();
                while (buffer.hasRemaining()) {
                    writeCount++;
                    socketChannel.write(buffer);
                }
            } catch (IOException e) {
                logger.error("Fail to write message to socket", e);
                cancelChannel(socketChannel);
            } finally {
                writeLock.unlock();
            }
        }

        private void flushOutputBuffer() {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            writeOutputBuffer.flip();
            writeToSocket(socketChannel, writeOutputBuffer);
            writeOutputBuffer.clear();
        }

        /** Cancel channel if there is IO exception. */
        private void cancelChannel(SocketChannel socketChannel) {
            logger.info(key + "; Read:count=" + readCount + ",avgsize=" + (readTotal / readCount) 
                    + "; Write:count=" + writeCount + ",avgsize=" + (writeTotal / writeCount));
            key.attach(null);
            key.cancel();
            try {
                socketChannel.close();
            } catch (IOException e) {
                logger.error("Fail to close socket", e);
            }
        }
    }

    /**
     * Leader-follower thread.
     */
    private static class Worker extends Thread {

        // lock to guard readable channels.
        private final static Lock boss = new ReentrantLock();

        private final static Lock assistant = new ReentrantLock();

        // A queue to store processing channels.
        private final static TaskQueue processingChannels = new TaskQueue();

        // A queue to store readable channels from select
        private final static Queue<SelectionKey> readyChannels = new LinkedList<SelectionKey>();

        // The main selector.
        private final Selector selector;

        // Server side handler
        private final ServerSPI serverSPI;

        private final ByteBuffer localBuffer;

        private final WriteStrategy writeStrategy;

        // Buffer for response.
        private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

        // Buffer for request.
        private final ByteBuffer recvBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_BUFFER_INITIAL_CAPACITY);

        int serialWriteSequence;

        public Worker(Selector selector, ServerSPI serverSPI, WriteStrategy writeStrategy, int poolSize) {
            this.selector = selector;
            this.serverSPI = serverSPI;
            this.writeStrategy = writeStrategy;
            localBuffer = WriteStrategy.BULK_WRITE == writeStrategy ? ByteBuffer
                    .allocateDirect(ServerConstants.WRITE_BUFFER_SIZE) : null;
        }

        // Accept connection
        void accept(SelectionKey key) {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            SocketChannel socketChannel = null;
            try {
                // Accept the connection and make it non-blocking
                socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(false);
                Socket socket = socketChannel.socket();
                if (ServerConstants.TCP_NODELAY) {
                    socket.setTcpNoDelay(true);
                }
                socket.setSendBufferSize(ServerConstants.SOCKET_SEND_BUFFER_SIZE);
                socket.setReceiveBufferSize(ServerConstants.SOCKET_RECV_BUFFER_SIZE);
                logger.info("socket recv buffer size is set to " + socket.getReceiveBufferSize() + " bytes");
                socket.setSendBufferSize(ServerConstants.SOCKET_SEND_BUFFER_SIZE);
                socket.setReceiveBufferSize(ServerConstants.SOCKET_RECV_BUFFER_SIZE);
                logger.info("socket send buffer size is set to " + socket.getSendBufferSize() + " bytes");
                SelectionKey readKey = socketChannel.register(selector, SelectionKey.OP_READ);
                readKey.attach(new ChannelBuffer(readKey, protocolSPI.headerLength()));
            } catch (IOException e) {
                logger.error("Fail to accept connection.", e);
                if (socketChannel != null) {
                    try {
                        socketChannel.close();
                    } catch (IOException ee) {
                        logger.error("Fail to close socket.", ee);
                    }
                }
                key.attach(null);
                key.cancel();
            }
        }

        /**
         * Check whether exist ready channel.
         */
        private TaskQueue.Node checkReadyChannel() {
            SelectionKey key;
            while ((key = readyChannels.poll()) != null) {
                if (key.isValid() && key.isReadable()) {
                    return processingChannels.offer((ChannelBuffer) key.attachment());
                } else if (key.isValid() && key.isAcceptable()) {
                    accept(key);
                }
            }
            return null;
        }

        /**
         * Get an available channel which contain requests.
         * 
         * @param node
         *            The node has been processed by current thread
         * @param isBlockWait
         *            Whether block or not if there is no ready channel.
         * @return An node which contains request. return null if there is no available channel.
         */
        private TaskQueue.Node getAvailableChannel(TaskQueue.Node node, boolean isBlockWait) {
            if (isBlockWait) {
                boss.lock();
            } else if (!boss.tryLock()) {
                return null;
            }
            try {
                if (node != null && node.item != null) {
                    processingChannels.remove(node);
                }
                if ((node = checkReadyChannel()) == null) {
                    if ((node = processingChannels.poll()) == null) {
                        if ((isBlockWait && selector.select() > 0) || (!isBlockWait && selector.selectNow() > 0)) {
                            readyChannels.addAll(selector.selectedKeys());
                            selector.selectedKeys().clear();
                        }
                        node = checkReadyChannel();
                    }
                }
            } catch (IOException e) {
                logger.error("Fail to select ready channel.", e);
            } finally {
                boss.unlock();
            }
            return node;
        }

        /**
         * Main method.
         */
        public void run() {
            logger.info("Start " + this + ", Write strategy:" + writeStrategy);
            TaskQueue.Node node = null;
            final boolean isBatchWrite = writeStrategy == WriteStrategy.SERIAL_WRITE
                    || writeStrategy == WriteStrategy.BULK_WRITE;
            while (running) {
                
                node = getAvailableChannel(node, true);
                ChannelBuffer channelBuffer;
                if (node == null || ((channelBuffer = node.item) == null)) {
                    continue;
                }

                serialWriteSequence = node.writeSequence;
                processMessages(channelBuffer, node.sequence);

                while (isBatchWrite) {
                    assistant.lock();
                    try {
                        node = getAvailableChannel(node, false);
                    } finally {
                        assistant.unlock();
                    }
                    ChannelBuffer lastChannelBuffer = channelBuffer;
                    if (node == null || (channelBuffer = node.item) != lastChannelBuffer) {
                        // Only force flush if the channel buffer changed.
                        if (writeStrategy == WriteStrategy.SERIAL_WRITE) {
                            lastChannelBuffer.ensureFlush(serialWriteSequence);
                        } else {
                            lastChannelBuffer.ensureFlush(localBuffer, true);
                        }
                    }

                    if (node == null || channelBuffer == null) {
                        break;
                    }
                    
                    processMessages(channelBuffer, node.sequence);
                }
            }
        }

        /**
         * Process all message of the channel buffer.
         * 
         * @param channelBuffer
         *            Channel buffer
         * @param sequence
         *            the read sequence when get the channel
         * 
         * @return whether flushed or not.
         */
        private void processMessages(final ChannelBuffer channelBuffer, final int sequence) {
            int retCode;
            while ((retCode = channelBuffer.readMessage(recvBuffer, sequence)) >= 0) {
                sendBuffer.clear();
                recvBuffer.flip();
                serverSPI.process(recvBuffer, sendBuffer);
                recvBuffer.clear();
                switch (writeStrategy) {
                case OPTIMIZED_SYNC_WRITE:
                    channelBuffer.optimizedSyncWrite(sendBuffer);
                    break;
                case SERIAL_WRITE:
                    serialWriteSequence = channelBuffer.serialWrite(sendBuffer);
                    break;
                case BULK_WRITE:
                    channelBuffer.bulkWrite(localBuffer, sendBuffer);
                    break;
                default:
                    channelBuffer.syncWrite(sendBuffer);
                }
                if (retCode == 0) {
                    break;
                }
            }
        }
    }

    /**
     * An bi-direction queue to store the tasks.
     */
    private static final class TaskQueue {
        static class Node {
            ChannelBuffer item;
            Node next;
            Node previous;
            final int sequence; // It was used to determine dirty buffer.
            final int writeSequence; // It was used to determine dirty buffer.

            Node(ChannelBuffer x, int sequence, int writeSequence) {
                item = x;
                this.sequence = sequence;
                this.writeSequence = writeSequence;
            }
        }

        /** Head of linked list */
        private final Node head;

        /** Tail of linked list */
        private Node last;

        /** Current node of the queue */
        private Node current;

        public TaskQueue() {
            current = last = head = new Node(null, 0, 0);
        }

        /**
         * Add the channel buffer and return the node.
         * 
         * @param e
         * @return
         */
        public Node offer(ChannelBuffer e) {
            Node t = last;
            last = last.next = new Node(e, e.readSequence, e.writeSequence);
            last.previous = t;
            return last;
        }

        /**
         * Remove the node from the queue.
         * 
         * @param n
         */
        public void remove(Node n) {
            if (n == last) {
                last = n.previous;
                last.next = null;
                if (n == current) {
                    current = head;
                }
            } else {
                n.next.previous = n.previous;
                n.previous.next = n.next;
                if (n == current) {
                    current = n.previous;
                }
            }
            n.item = null;
        }

        /**
         * Return the current node and change the current node to next.
         * 
         * @return the current node
         */
        public Node poll() {
            if (current.next == null) {
                return null;
            }
            Node n = current = current.next;
            if (current == last) {
                current = head;
            }
            return n;
        }
    }
}