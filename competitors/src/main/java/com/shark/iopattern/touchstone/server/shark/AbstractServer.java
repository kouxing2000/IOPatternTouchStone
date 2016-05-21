/**
 * 
 */
package com.shark.iopattern.touchstone.server.shark;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.ChannelHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shark.iopattern.touchstone.server.ServerIF;
import com.shark.iopattern.touchstone.share.ProtocolSPI;


/**
 * @author weili1
 * 
 */
public abstract class AbstractServer implements ServerIF {
	
	volatile boolean started_ = false;

	final Logger logger = LoggerFactory.getLogger(getClass());

	private ServerSocketChannel serverChannel;

	private Thread bossThread;

	ServerSPI serverSPI;

	ProtocolSPI protocolSPI;
	
	Selector selector ;

	List<Thread> runningThreads = new ArrayList<Thread>();
	

	public AbstractServer() {
		

	}
	
	protected ChannelHolder createChannelHolder(SocketChannel channel,
												SelectionKey key) {
		return new BufferWriteChannelHolder(key, channel, protocolSPI);
	}

	@Override
	public void start() throws Exception {
		
		if (started_) {
			throw new IllegalStateException("Pool already started.");
		}
		
		started_ = true;
		
		serverChannel = ServerSocketChannel.open();
		ServerSocket socket = serverChannel.socket();
		socket.setReceiveBufferSize(ServerConstants.SOCKET_RECV_BUFFER_SIZE);

		socket.bind(new InetSocketAddress(ServerConstants.SERVER_IP,
				ServerConstants.SERVER_PORT));

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
								ServerSocketChannel server = (ServerSocketChannel) key
										.channel();
								SocketChannel channel = server.accept();

								registerChannel(selector, channel, key);
							}

							// is there data to read on this channel?
							if (key.isReadable()) {
								readReady(key);
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

	@Override
	public void stop() throws Exception {
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

	}

	/**
	 * Register the given channel with the given selector for the given
	 * operations of interest
	 */
	protected void registerChannel(Selector selector, SocketChannel channel,
			SelectionKey key) throws Exception {
		if (channel == null) {
			return; // could happen
		}

		logger.info("New Channel:" + channel);

		Socket socket = channel.socket();
		channel.configureBlocking(ServerConstants.BLOCKING_MODE);
		if (ServerConstants.TCP_NODELAY) {
			socket.setTcpNoDelay(true);
			logger.info("DiameterServer TCP_NODELAY is ON");
		}
		logger.info("socket recv buffer size is set to "
				+ socket.getReceiveBufferSize() + " bytes");
		socket.setSendBufferSize(ServerConstants.SOCKET_SEND_BUFFER_SIZE);
		logger.info("socket send buffer size is set to "
				+ socket.getSendBufferSize() + " bytes");

		// register it with the selector
		key = channel.register(selector, SelectionKey.OP_READ);

		// new ChannelHolder

		ChannelHolder channelHodler = createChannelHolder(channel, key);

		key.attach(channelHodler);
	}

	// ----------------------------------------------------------

	protected void readReady(SelectionKey key) throws Exception {

		BufferWriteChannelHolder proc = (BufferWriteChannelHolder) key.attachment();

		if (proc == null) {
			throw new RuntimeException();
		}

		//remove the read interest
		int interestOps = key.interestOps();
		interestOps &= ~SelectionKey.OP_READ;
		key.interestOps(interestOps);

		registerReadableChannel(proc);
	}

	protected abstract void registerReadableChannel(BufferWriteChannelHolder proc) throws Exception;

	// ----------------------------------------------------------

}
