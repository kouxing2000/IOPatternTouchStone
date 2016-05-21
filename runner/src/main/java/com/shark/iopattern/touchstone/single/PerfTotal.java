/**
 * 
 */
package com.shark.iopattern.touchstone.single;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import com.shark.iopattern.touchstone.agent.Commander;
import com.shark.iopattern.touchstone.agent.ParameterDataPair;
import com.shark.iopattern.touchstone.agent.ParameterDataSet;
import com.shark.iopattern.touchstone.client.PerfClient;
import com.shark.iopattern.touchstone.server.PerfServer;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.XppDriver;

/**
 * @author weili1
 * 
 */
public class PerfTotal {
	public static void main(String[] args) throws Throwable {
		
//		pdInit();
		
		PerfServer.main(args);
		
		PerfClient.main(args);
		
		Commander.main(new String[]{"parameters.json"});
		
		System.exit(0);

	}
	
	public static void pdInit() {
		ParameterDataSet pd = new ParameterDataSet();
		
		String serverURL = "http://localhost:1234/";
		String clientURL = "http://localhost:2345/";
		
		pd.serverURL = serverURL;
		pd.clientURL = clientURL;
		
		pd.parameterDataPairs = new ArrayList<ParameterDataPair>();

		ParameterDataPair pdp = new ParameterDataPair();

		pd.parameterDataPairs.add(pdp);
		
		Map<String, String> map;
		map = new HashMap<String, String>();
		map.put("a", "b");
		map.put("c", "d");
		
		pdp.initServerParameters = map;
		
		map = new HashMap<String, String>();
		map.put("a", "b");
		map.put("c", "d");
		
		
		pdp.serverParameters = new ArrayList<Map<String, String>>();
		pdp.serverParameters.add(map);

		map = new HashMap<String, String>();
		map.put("a", "b");
		map.put("c", "d");
		pdp.clientParameters = new ArrayList<Map<String, String>>();
		
		pdp.clientParameters.add(map);

		map = new HashMap<String, String>();
		map.put("a", "b");
		map.put("c", "d");
		pdp.clientParameters.add(map);
		
		XStream xstream = new XStream(new XppDriver());
		String xmlPD = xstream.toXML(pd);
		System.out.println(xmlPD);
		
		try {
			File file = new File("parameters_new.xml");
			file.createNewFile();
			xstream.toXML(pd, new FileOutputStream(file));
			xstream.fromXML(new FileInputStream(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
