package com.shark.iopattern.touchstone.server;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ServerConstants {

    private static Logger logger = LoggerFactory.getLogger(ServerConstants.class);

    /**
     * The following two are normal parameters, no performance related
     */
    public static String SERVER_IP = "127.0.0.1";

    public static int SERVER_PORT = 1812;

    /**
     * The following two used in Agent Mode
     */
    public static int MANAGE_PORT = 1234;

    public static boolean MANAGED = false;

    public static String ProtocolSPI = "";

    /**
     * The following are server performance parameters
     */
    public static String ServerSPI = "";

    public static String SERVER_TYPE = "";

    public static int READ_BUFFER_SIZE = (64 * 2048);

    public static int WRITE_BUFFER_SIZE = (64 * 2048);

    public static int THREAD_POOL_SIZE = 2;

    // QAS
    public static int READ_THREAD_POOL_SIZE = 1;

    // QAS, LFS
    public static int WRITE_THREAD_POOL_SIZE = 0;

    // QAS
    public static int MAX_READ_TIMES_PER_THREAD_CHANNEL = 3;

    // QAS
    public static int QUEUE_SIZE = 1000;

    // Netty
    public static int IO_THREAD_POOL_SIZE = 2;

    public static int BATCH_PROCESS_NUMBER = 1;

    /**
     * The following are the socket options
     */
    public static int SOCKET_RECV_BUFFER_SIZE = (64 * 2048);

    public static int SOCKET_SEND_BUFFER_SIZE = (64 * 2048);

    public static boolean TCP_NODELAY = false;

    public static boolean BLOCKING_MODE = false;

    /**
     * The following are used by Protocol Implementation
     */
    public static int SERVER_LOGIC_COMPLEXITY = 125;

    /**
     * time unit is millisecond
     */
    public static int SERVER_LOGIC_SLEEP_TIME = 0;

    private static Map<String, Field> staticFields = new HashMap<String, Field>();

    static {
        for (Field field : ServerConstants.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
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

    public static Map<String, String> set(Map<String, String> parameters) throws Exception {

        Map<String, String> result = new HashMap<String, String>(parameters.size());

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue().trim();

            Field field = staticFields.get(name);

            if (field == null) {
                logger.warn("Field:" + name + " Not Exist, please Check!");
                continue;
            }

            // put the previous value in
            result.put(name, field.get(null) == null ? null : field.get(null).toString());

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

    public static String export() {
        StringBuffer buffer = new StringBuffer();
        for (Field field : ServerConstants.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                try {
                    buffer.append("" + field.getName() + "=" + field.get(null) + "\n");
                } catch (IllegalArgumentException e) {
                } catch (IllegalAccessException e) {
                }
            }
        }
        return (buffer.toString());

    }

    private ServerConstants() {
        // NOOP.
    }

    public static void main(String[] args) throws Throwable {
        System.out.println(export());
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("MESSAGE_BUFFER_INITIAL_CAPACITY", "2048");
        set(parameters);
    }
}
