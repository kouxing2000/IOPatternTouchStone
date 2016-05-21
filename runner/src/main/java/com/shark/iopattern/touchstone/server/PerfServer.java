/**
 * 
 */
package com.shark.iopattern.touchstone.server;

import com.shark.iopattern.touchstone.client.ClientConstants;
import com.shark.iopattern.touchstone.client.ClientSPI;
import com.shark.iopattern.touchstone.server.PerfServerServlet;
import com.shark.iopattern.touchstone.server.ServerConstants;
import com.shark.iopattern.touchstone.server.ServerSPI;
import com.shark.iopattern.touchstone.share.ProtocolSPI;
import com.shark.iopattern.touchstone.variable_length.VLProtocol;
import com.shark.iopattern.touchstone.variable_length.VLServerSPI2;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * @author weili1
 * 
 */
public class PerfServer {

    /**
     * @param args
     * @throws Throwable
     */
    public static void main(String[] args) throws Throwable {

        if (args.length == 0) {
            args = new String[] { "server.properties" };
        }

        if (args.length >= 1) {
            try {
                ServerConstants.loadProperties(args[0]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        PerfServerServlet perfServer = new PerfServerServlet();

        if (!ServerConstants.MANAGED) {
            perfServer.setProtocolSPI((ProtocolSPI) Class.forName(ServerConstants.ProtocolSPI).newInstance());
            perfServer.setServerSPI((ServerSPI) Class.forName(ServerConstants.ServerSPI).newInstance());

            perfServer.start();
        } else {
            Server jetty = new Server(1234);
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            context.addServlet(new ServletHolder(perfServer), "/*");
            jetty.setHandler(context);
            try {
                jetty.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
