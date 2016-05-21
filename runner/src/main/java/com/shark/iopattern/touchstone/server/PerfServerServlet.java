package com.shark.iopattern.touchstone.server;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.shark.iopattern.touchstone.agent.CPUMonitor;
import com.shark.iopattern.touchstone.agent.SystemLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caucho.hessian.server.HessianServlet;
import com.shark.iopattern.touchstone.share.ProtocolSPI;
import com.shark.iopattern.touchstone.agent.Report;
import com.shark.iopattern.touchstone.agent.Service;

public class PerfServerServlet extends HessianServlet implements Service {
    private static Logger logger = LoggerFactory.getLogger(PerfServerServlet.class);

    SystemLoad systemLoad = new SystemLoad();

    CPUMonitor cpuMonitor = new CPUMonitor();

    @Override
    public SystemLoad getSystemLoad() {
        systemLoad.setCpuUsage(cpuMonitor.getTotalUsage());
        return systemLoad;
    }

    private ServerSPI serverSPI;

    private ProtocolSPI protocolSPI;

    private AtomicLong messageCounter = new AtomicLong();

    public void setServerSPI(final ServerSPI serverSPI) {

        this.serverSPI = new ServerSPI() {
            @Override
            public void process(ByteBuffer in, ByteBuffer out) {
                serverSPI.process(in, out);
                messageCounter.incrementAndGet();
            }
        };
    }

    public void setProtocolSPI(ProtocolSPI protocolSPI) {
        this.protocolSPI = protocolSPI;
    }

    private boolean running = false;

    private ServerIF server;

    @Override
    public void start() throws Exception {

        logger.info("Start...");

        running = true;

        clearCounters();

        Class<ServerIF> serverIFClass = (Class<ServerIF>) Class.forName(ServerConstants.SERVER_TYPE);

        Constructor<ServerIF> constructor =  serverIFClass.
                getConstructor(ServerSPI.class, ProtocolSPI.class);

        server = constructor.newInstance(serverSPI, protocolSPI);

        server.start();

    }

    @Override
    public void stop() throws Exception {

        logger.info("Sop...");

        running = false;

        server.stop();

        for (int i = 0; i < 2; i++) {
            System.gc();
        }

    }

    @Override
    public void clearCounters() {
        messageCounter.set(0);
    }

    @Override
    public String getStatusInfo() {
        return "[Processed:" + messageCounter.get() + "]";
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public Report getReport() {
        return null;
    }

    @Override
    public String getParameters() {
        return ServerConstants.export();
    }

    private Map<String, String> previousSetting;

    @Override
    public void set(Map<String, String> parameters) throws Exception {
        previousSetting = ServerConstants.set(parameters);
        logger.info("Push Parameters:" + parameters);

        setProtocolSPI((ProtocolSPI) Class.forName(ServerConstants.ProtocolSPI).newInstance());
        setServerSPI((ServerSPI) Class.forName(ServerConstants.ServerSPI).newInstance());

    }

    @Override
    public void rollbackPreviousSet() throws Exception {
        if (previousSetting == null) {
            return;
        }
        ServerConstants.set(previousSetting);
        logger.info("rollback Parameters:" + previousSetting);
    }

}
