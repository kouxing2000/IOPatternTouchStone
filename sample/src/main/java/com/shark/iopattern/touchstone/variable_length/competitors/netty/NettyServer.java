/**
 * 
 */
package com.shark.iopattern.touchstone.variable_length.competitors.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.shark.iopattern.touchstone.server.ServerIF;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.server.ServerConstants;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shark.iopattern.touchstone.share.ProtocolSPI;


/**
 * @author weili1
 * 
 */
public class NettyServer implements ServerIF {

	private static Logger logger = LoggerFactory.getLogger(NettyServer.class);
	
	private ServerSPI serverSPI;

	private ProtocolSPI protocolSPI;
	
	public NettyServer(ServerSPI serverSPI, ProtocolSPI protocolSPI) {
		this.serverSPI = serverSPI;
		this.protocolSPI = protocolSPI;
	}
	
	private ServerBootstrap bootstrap ;
	
	private MyPipelineFactory pipelineFactory;
	
	public void start(){
		// Configure the server.
		// ServerBootstrap bootstrap = new ServerBootstrap(new
		// OioServerSocketChannelFactory(Executors
		// .newCachedThreadPool(), Executors.newCachedThreadPool()));
		bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(Executors
						.newCachedThreadPool(), Executors.newCachedThreadPool(), ServerConstants.IO_THREAD_POOL_SIZE));

		pipelineFactory = new MyPipelineFactory();
		
		// Set up the default event pipeline.
		bootstrap.setPipelineFactory(pipelineFactory);
		
		bootstrap.setOption("receiveBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(ServerConstants.READ_BUFFER_SIZE));
		bootstrap.setOption("writeBufferLowWaterMark", String.valueOf(ServerConstants.WRITE_BUFFER_SIZE / 2));
		bootstrap.setOption("writeBufferHighWaterMark", String.valueOf(ServerConstants.WRITE_BUFFER_SIZE));

		try {
			logger.info(pipelineFactory.getPipeline().toString());
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(ServerConstants.SERVER_IP,
				ServerConstants.SERVER_PORT));

		bootstrap.setOption(SEND_BUFFER_SIZE, ""
				+ ServerConstants.SOCKET_SEND_BUFFER_SIZE);
		bootstrap.setOption(RECEIVE_BUFFER_SIZE, ""
				+ ServerConstants.SOCKET_RECV_BUFFER_SIZE);
		bootstrap.setOption("child." + TCP_NO_DELAY, ""
				+ ServerConstants.TCP_NODELAY);

		logger.info(" TCP_NODELAY is "
				+ ServerConstants.TCP_NODELAY);
		logger.info(" socket recv buffer size is set to "
				+ ServerConstants.SOCKET_RECV_BUFFER_SIZE + " bytes");
		logger.info(" socket send buffer size is set to "
				+ ServerConstants.SOCKET_SEND_BUFFER_SIZE + " bytes");
	}

	public void stop(){
		
		bootstrap.releaseExternalResources();
		
		pipelineFactory.dispose();
		
	}
	
	private class MyPipelineFactory implements ChannelPipelineFactory {

		SimpleChannelUpstreamHandler handler;
		
		ExecutionHandler executionHandler;
		
		ExecutorService executor ;
		
		public MyPipelineFactory() {
			
			handler = new NettyHandler(serverSPI);
			 
			if (ServerConstants.THREAD_POOL_SIZE > 1) {

				int threadNum = ServerConstants.THREAD_POOL_SIZE;
				
				final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(
						ServerConstants.QUEUE_SIZE);
				
				executor = new ThreadPoolExecutor(threadNum,
						threadNum, 0L, TimeUnit.MILLISECONDS, queue, new RejectedExecutionHandler(){
					@Override
					public void rejectedExecution(Runnable r,
							ThreadPoolExecutor executor) {
						//logger.info("Queue is Full, wait it until empty");
						try {
							queue.put(r);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						//logger.info("Finally put it into Queue!");
					}
				});
				
				executionHandler = new ExecutionHandler(executor);
			}
			
		}

		public ChannelPipeline getPipeline() {

			ChannelPipeline pipeLine = new DefaultChannelPipeline();

			pipeLine.addLast("divide", new MessageDivideHandler(protocolSPI));

			if (executionHandler != null) {
				pipeLine.addLast("exec", executionHandler);
			}

			pipeLine.addLast("process", handler);

			return pipeLine;
		}
		
		public void dispose(){
			if (executor!=null){
				executor.shutdown();
			}
		}

	}
	
	public static final String RECEIVE_BUFFER_SIZE = "receiveBufferSize";

	public static final String SEND_BUFFER_SIZE = "sendBufferSize";

	public static final String TCP_NO_DELAY = "tcpNoDelay";

}
