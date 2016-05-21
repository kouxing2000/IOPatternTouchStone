package com.shark.iopattern.touchstone.client;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClientConstants {

	private static Logger logger = LoggerFactory
			.getLogger(ClientConstants.class);

	/**
	 * The following two are normal parameters, no performance related
	 */
	public static String SERVER_IP = "127.0.0.1";

	public static int SERVER_PORT = 1812;

	public static String ProtocolSPI = "";
	public static String ClientSPI = "";

	/**
	 * These two is for the latency output
	 */
	public static int LATENCY_BUCKETS = 10;

	public static int LATENCY_BUCKET_SIZE = 30;
	
	public static double MOST_DEFINITION = 0.999;
	
	/**
     * The following two used in Agent Mode
     */
    public static int MANAGE_PORT = 2345;
    
    public static boolean MANAGED = false;
    
    /**
     * The following are client performance test parameters
     */
	public static int WARM_UP_NUM_MESSAGES = 10;

	public static int NUM_MESSAGES = 10000;

	public static int READ_BUFFER_SIZE = (64 * 1024);

	public static int MAX_PENDING_REQUESTS_PER_CONNECTION = 10;

	public static int CONNECTION_NUM = 5;

	public static int TEST_TIMES = 3;

    /**
     * The following are the socket options
     */
	public static int SOCKET_RECV_BUFFER_SIZE = (64 * 2048);

	public static int SOCKET_SEND_BUFFER_SIZE = (64 * 2048);

	public static boolean TCP_NODELAY = false;
	
    /**
     * This is used by Protocol Implementation, at present, the RESPONSE_SIZE is equal to it.
     */
	public static int REQUEST_SIZE = 512;
    
    private static Map<String, Field> staticFields = new HashMap<String, Field>();
    
    static{
        for(Field field:ClientConstants.class.getFields()){
        	if (Modifier.isStatic(field.getModifiers())){
        		staticFields.put(field.getName(), field);
        	}
        }
    }

    public static void loadProperties(String fileName) throws Exception {
        logger.info("try to read from: " + fileName);
        loadProperties(ConfigurationUtils.locate(fileName).openStream());
    }
    
    public static void loadProperties(InputStream is) throws Exception {
        Properties props = new Properties();
        props.load(is);
        
        set((Map) props);

        logger.info(export());
    }
	
	public static Map<String,String> set(Map<String,String> parameters) throws Exception{
		
	    Map<String,String> result = new HashMap<String, String>(parameters.size());

	       
		for (Map.Entry<String, String> entry: parameters.entrySet()){
			String name = entry.getKey();
			String value = entry.getValue().trim();
			
			Field field = staticFields.get(name);
			
			if (field == null){
				throw new IllegalArgumentException("Field:" + name + " Not Exist, please Check!");
			}
			
			//put the previous value in
            result.put(name, field.get(null).toString());
			
			if (field.getType() == int.class) {
				field.set(null, new Integer(value));
			} else if (field.getType() == boolean.class) {
				field.set(null, new Boolean(value));
			} else if (field.getType() == long.class) {
				field.set(null, new Long(value));
			} else {
				// Only other supported type is string
				field.set(null, value);
			}
		}
		
		return result;
	}
	
    public static String export(){
        StringBuffer buffer = new StringBuffer();
        for(Field field:ClientConstants.class.getFields()){
        	if (Modifier.isStatic(field.getModifiers())){
        		try {
					buffer.append(field.getName() + "=" + field.get(null) + "\n");
				} catch (IllegalArgumentException e) {
				} catch (IllegalAccessException e) {
				}
        	}
        }
        return(buffer.toString());
    	 
    }

	private ClientConstants() {
		// NOOP.
	}
}
