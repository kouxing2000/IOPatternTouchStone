package com.shark.iopattern.touchstone.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.shark.iopattern.touchstone.agent.CPUMonitor;
import com.shark.iopattern.touchstone.share.ChannelHolder;
import com.shark.iopattern.touchstone.agent.SystemLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.caucho.hessian.server.HessianServlet;
import com.shark.iopattern.touchstone.share.DefaultChannelHolder;
import com.shark.iopattern.touchstone.share.ProtocolSPI;
import com.shark.iopattern.touchstone.agent.Report;
import com.shark.iopattern.touchstone.agent.Service;

public class PerfClientServlet extends HessianServlet implements Service {

	private static Logger logger = LoggerFactory.getLogger(PerfClientServlet.class);

	private boolean running = false;

	private ResultConclude[] results;
	private double[] throughputs;

	private Thread bossThread;

	private SystemLoad systemLoad = new SystemLoad();

	private CPUMonitor cpuMonitor = new CPUMonitor();
	
	private List<AsyncReciever> receivers = new ArrayList<AsyncReciever>();
	private List<AsyncSender> senders = new ArrayList<AsyncSender>();

	@Override
	public SystemLoad getSystemLoad() {
		systemLoad.setCpuUsage(cpuMonitor.getTotalUsage());
		return systemLoad;
	}

	@Override
	public void start() throws Exception {
		
		logger.info("Start...");

		clearCounters();

		running = true;

		clientSPI.init();
		
		bossThread = new Thread() {
			@Override
			public void run() {

				try {
					
					int connectionNum = ClientConstants.CONNECTION_NUM;

					results = new ResultConclude[ClientConstants.TEST_TIMES];
					throughputs = new double[ClientConstants.TEST_TIMES];

					for (int k = 0; k < ClientConstants.TEST_TIMES; k++) {

						logger.info("\n++++++++++++\n" + "Start the " + (k + 1)
								+ " time test!" + "\n++++++++++++++");

						ResultConclude rc = new ResultConclude();
						results[k] = rc;

						CountDownLatch startSignal = new CountDownLatch(
								connectionNum);
						CountDownLatch doneSignal = new CountDownLatch(
								connectionNum);

						for (int i = 0; i < connectionNum; i++) {

							logger.info("Starting ASYC test. Connection:" + i);

							SocketChannel channel = connect();

							// new channelholder
							ChannelHolder channelHolder = new DefaultChannelHolder(
									channel, protocolSPI,
									ClientConstants.READ_BUFFER_SIZE);

							PendingManager pendinger = new PendingManager(
									ClientConstants.MAX_PENDING_REQUESTS_PER_CONNECTION);
							
							// Start the reciever.
							AsyncReciever r = new AsyncReciever("RECV_" + i,
									channelHolder, rc, pendinger, startSignal,
									doneSignal, protocolSPI, clientSPI);
							receivers.add(r);

							// Start the sender.
							AsyncSender s = new AsyncSender("SEND_" + i,
									channelHolder, pendinger, startSignal,
									clientSPI);
							senders.add(s);

							r.setStartTimes(s.getStartTimes());

							r.start();

							s.start();

						}

						startSignal.await();
						long startTime = System.currentTimeMillis();
						logger.info("Ready To Go!");

						doneSignal.await();
						long doneTime = System.currentTimeMillis();
						logger.info("All Done!");

						//wait the Receiver calculate the latency
						while(rc.getAddNum() < ClientConstants.CONNECTION_NUM){
							Thread.sleep(10);
						}
						
						rc.finish(doneTime - startTime);
						throughputs[k] = rc.getThroughput();

					}
					
					running = false;

					Arrays.sort(throughputs);

					logger.info(Arrays.toString(throughputs));

					logger.info(getReport().toString());

					if (results.length >= 3) {
						double sum = 0;
						for (int i = 1; i < results.length - 1; i++) {
							sum += results[i].getThroughput();
						}

						logger
								.info("After remove the biggest and the smallest, the average throughput is "
										+ (sum * 1d / (results.length - 2)));
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					running = false;
				}
			}
		};

		bossThread.start();

	}

	@Override
	public void stop() throws Exception {
		logger.info("Stop...");
		
		running = false;

		if (bossThread != null) {
			bossThread.interrupt();
		}

		for (int i = 0; i < 2; i++) {
			System.gc();
			Thread.sleep(1000);
		}

	}
	
	@Override
	public void clearCounters() {
		
		results = new ResultConclude[0];
		
		receivers.clear();
		
		senders.clear();
	}
	
	@Override
	public String getStatusInfo() {
		
		long sendTotal = 0;
		for (AsyncSender as : senders) {
			sendTotal += as.numMessages;
		}
		
		long recvTotal = 0;
		for (AsyncReciever ar : receivers) {
			recvTotal += ar.numMessages;
		}
		
		double percent = 1d * recvTotal / (ClientConstants.CONNECTION_NUM * ClientConstants.NUM_MESSAGES * ClientConstants.TEST_TIMES);
		
		return "[Send:" + sendTotal + " Recv:" + recvTotal + " Pending:" + (sendTotal - recvTotal) + " Percent:" + ((int) (percent*100)) + "%]";
	}

	@Override
	public Report getReport() {

		ResultConclude rc = results[0];

		for (int i = 1; i < results.length; i++) {
			if (results[i].getThroughput() > rc.getThroughput()) {
				rc = results[i];
			}
		}

		Report result = new Report();
		result.setAverageLatency(rc.getAverageLatency());
		result.setThroughput(rc.getThroughput());
		result.setMostBelowLatency(rc.getMostBelowLatency());

		return result;
	}

	@Override
	public String getParameters() {
		return ClientConstants.export();
	}
	
    private Map<String,String> previousSetting;

	@Override
	public void set(Map<String, String> parameters) throws Exception {
	    previousSetting = ClientConstants.set(parameters);
		logger.info("Push Parameters:" + parameters);

		setProtocolSPI((ProtocolSPI) Class.forName(ClientConstants.ProtocolSPI).newInstance());
		setClientSPI((ClientSPI) Class.forName(ClientConstants.ClientSPI).newInstance());

	}
	   
    @Override
    public void rollbackPreviousSet() throws Exception {
        if (previousSetting == null){
            return;
        }
        ClientConstants.set(previousSetting);
        logger.info("rollback Parameters:" + previousSetting);
    }

	@Override
	public boolean isRunning() {
		return running;
	}

	private ClientSPI clientSPI;

	private ProtocolSPI protocolSPI;

	public void setClientSPI(ClientSPI clientSPI) {
		this.clientSPI = clientSPI;
	}

	public void setProtocolSPI(ProtocolSPI protocolSPI) {
		this.protocolSPI = protocolSPI;
	}

	private static SocketChannel connect() throws IOException,
			UnknownHostException, SocketException {
		SocketChannel channel = SocketChannel.open();
		SocketAddress addr = new InetSocketAddress(ClientConstants.SERVER_IP,
				ClientConstants.SERVER_PORT);
		Socket socket = channel.socket();
		socket.setReceiveBufferSize(ClientConstants.SOCKET_RECV_BUFFER_SIZE);
		socket.setSendBufferSize(ClientConstants.SOCKET_SEND_BUFFER_SIZE);
		logger
				.debug("PerfClientServlet TCP_NODELAY is "
						+ ClientConstants.TCP_NODELAY);
		if (ClientConstants.TCP_NODELAY) {
			socket.setTcpNoDelay(true);
		}
		channel.connect(addr);
		logger.debug("PerfClientServlet socket recv buffer size is set to "
				+ socket.getReceiveBufferSize() + " bytes");
		logger.debug("PerfClientServlet socket send buffer size is set to "
				+ socket.getSendBufferSize() + " bytes");
		return channel;
	}

}