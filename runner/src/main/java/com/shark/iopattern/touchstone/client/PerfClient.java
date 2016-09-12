/**
 * 
 */
package com.shark.iopattern.touchstone.client;

import com.shark.iopattern.touchstone.share.ProtocolSPI;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


/**
 * @author weili1
 * 
 */
public class PerfClient {

	/**
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {

		if (args.length == 0) {
			args = new String[] { "client.properties" };
		}

		if (args.length >= 1) {
			try {
				ClientConstants.loadProperties(args[0]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		PerfClientServlet perfClientServlet = new PerfClientServlet();

		if (!ClientConstants.MANAGED) {
			perfClientServlet.setProtocolSPI((ProtocolSPI) Class.forName(ClientConstants.ProtocolSPI).newInstance());
			perfClientServlet.setClientSPI((ClientSPI) Class.forName(ClientConstants.ClientSPI).newInstance());

			perfClientServlet.start();
		} else {

			Server jetty = new Server(2345);
			ServletContextHandler context = new ServletContextHandler(
					ServletContextHandler.SESSIONS);
			context.setContextPath("/");
			context.addServlet(new ServletHolder(perfClientServlet), "/*");
			jetty.setHandler(context);
			try {
				jetty.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
