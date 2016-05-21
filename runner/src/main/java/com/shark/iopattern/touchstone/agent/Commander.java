/**
 *
 */
package com.shark.iopattern.touchstone.agent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.commons.configuration.ConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.caucho.hessian.client.HessianProxyFactory;

/**
 * @author weili1
 */
public class Commander {

    private static Logger logger = LoggerFactory.getLogger(Commander.class);

    private static DateFormat dateFormat = new SimpleDateFormat(
            "yyyyMMdd'_'HHmmss");

    /**
     * @param args
     * @throws Throwable
     */
    public static void main(String[] args) throws Throwable {

        logger.info(dateFormat.format(new Date()));

        if (args.length < 1) {
            args = new String[]{"parameters.xml"};
        }

        URL pdFile = ConfigurationUtils.locate(args[0]);

        if (pdFile == null) {
            System.err.println("No Parameters Data File Found!");
            System.exit(1);
        }

        ParameterDataSet pds = new Gson().fromJson(new InputStreamReader(pdFile.openStream()), ParameterDataSet.class);
        Commander commander = new Commander(pds);
        commander.start();
    }

    private ParameterDataSet pds;

    private Service serverService;

    private Service clientService;

    private File reportFile;

    public Commander(ParameterDataSet pds) throws Throwable {
        this.pds = pds;

        String serverURL = pds.serverURL;

        String clientURL = pds.clientURL;

        logger.info("serverURL:" + serverURL);
        logger.info("clientURL:" + clientURL);

        serverService = createService(serverURL);

        clientService = createService(clientURL);

        reportFile = new File("reports." + dateFormat.format(new Date())
                + ".html");
        reportFile.createNewFile();
        logger.info("Report File:" + reportFile.getAbsolutePath());

        reportFW = new FileWriter(reportFile);

        InetAddress[] addrs = Inet4Address.getAllByName(Inet4Address
                .getLocalHost().getHostName());
        logger.info(Arrays.toString(addrs));

        write("Agent Host:" + Arrays.toString(addrs) + "\n");

        write("serverURL:" + serverURL + "\n");
        write("clientURL:" + clientURL + "\n");

    }

    private FileWriter reportFW;

    private void write(String sentence) throws IOException {
        reportFW.write(sentence.replace("\n", "<BR/>"));
    }

    private void start() throws Throwable {

        try {

            logger.info("ParameterDataPair Number:" + pds.parameterDataPairs.size());

            int totalRound = 0;

            for (ParameterDataPair pd : pds.parameterDataPairs) {
                totalRound += pd.serverParameters.size() * pd.clientParameters.size();
            }

            logger.info("totalRound Number:" + totalRound);

            int papIndex = 0;

            int roundIndex = 0;

            for (ParameterDataPair pd : pds.parameterDataPairs) {

                papIndex++;

                logger.info("\n############\n" + "No." + papIndex + ":" + pd.name + "\n############\n");

                int totalRoundInPDP = pd.clientParameters.size()
                        * pd.serverParameters.size();

                logger.info("Round number of " + pd.name +
                        " :" + totalRoundInPDP);

                write("\n############\n");
                write("Test:" + pd.name + "");
                write("\n############\n");

                if (pd.initClientParameters != null) {
                    write("initClientParameters:" + pd.initClientParameters + "\n");
                    logger.info("initClientParameters:" + pd.initClientParameters + "\n");
                    clientService.set(pd.initClientParameters);
                }

                if (pd.initServerParameters != null) {
                    write("initServerParameters:" + pd.initServerParameters + "\n");
                    logger.info("initServerParameters:" + pd.initServerParameters + "\n");
                    serverService.set(pd.initServerParameters);
                }

                {
                    String serverParameters = serverService.getParameters();
                    String clientParameters = clientService.getParameters();
                    write("server init parameters:\n" + serverParameters + "\n");
                    write("client init parameters:\n" + clientParameters + "\n");
                }

                IndexItem[][] reportMatrix = new IndexItem[pd.serverParameters.size()][];

                for (int i = 0; i < reportMatrix.length; i++) {
                    reportMatrix[i] = new IndexItem[pd.clientParameters.size()];
                }

                int rowIndex = -1;
                int colIndex = -1;

                write("Result Matrix:\n");

                write("<table border=\"1\">");

                int roundNo = 0;

                write("<tr>");
                write("<td>");
                write("Server Parameters \\ Client Parameters");
                write("</td>");
                for (Map<String, String> clientParameters : pd.clientParameters) {
                    write("<td>");
                    write(clientParameters.toString());
                    write("</td>");
                }
                write("</tr>");

                for (Map<String, String> serverParameters : pd.serverParameters) {
                    rowIndex++;

                    write("<tr>");

                    write("<td>");
                    write(serverParameters.toString());
                    write("</td>");

                    logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
                    logger.info("Server Parameters:" + serverParameters);
                    logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

                    serverService.set(serverParameters);
                    serverService.start();

                    logger.info("Server Start");

                    colIndex = -1;
                    for (Map<String, String> clientParameters : pd.clientParameters) {
                        roundIndex++;

                        colIndex++;
                        write("<td>");

                        roundNo++;

                        logger.info("\n===============\nStart Round:"
                                + roundNo + " of " + totalRoundInPDP + "!\n===============");

                        serverService.clearCounters();

                        logger.info("Client Parameters:" + clientParameters);


                        clientService.set(clientParameters);
                        clientService.start();

                        logger.info("Client Start");

                        double sumServerCPU = 0;
                        double sumClientCPU = 0;

                        int count = 0;

                        while (clientService.isRunning()) {

                            count++;

                            //ignore the first time's value
                            if (count > 1) {
                                double serverCPU = serverService.getSystemLoad().getCpuUsage();
                                sumServerCPU += serverCPU;

                                double clientCPU = clientService.getSystemLoad().getCpuUsage();
                                sumClientCPU += clientCPU;
                            }

                            Thread.sleep(1000);

                            logger.info("Server Status:" + serverService.getStatusInfo());
                            logger.info("Client Status:" + clientService.getStatusInfo());
                        }

                        //cause ignored the first time's value
                        count--;

                        clientService.stop();
                        clientService.rollbackPreviousSet();
                        logger.info("Client Down");

                        double serverCPUUsage = sumServerCPU / count;
                        double clientCPUUsage = sumClientCPU / count;

                        Report clientReport = clientService.getReport();

                        clientService.clearCounters();

                        IndexItem indexItem = new IndexItem();
                        reportMatrix[rowIndex][colIndex] = indexItem;
                        indexItem.setAverageLatency(clientReport.getAverageLatency());
                        indexItem.setMostBelowLatency(clientReport.getMostBelowLatency());
                        indexItem.setThroughput(clientReport.getThroughput());
                        indexItem.setServerCPUUsage(serverCPUUsage);
                        indexItem.setClientCPUUsage(clientCPUUsage);

                        logger.info("\n===============\nFinish Round:"
                                + roundNo + "!\n===============");


                        logger.info("Result:\n" + indexItem);

                        write(indexItem.toString());

                        write("</td>");

                        logger.info("Total Process - " + roundIndex + "/" + totalRound + " Percent:" + (100 * roundIndex / totalRound) + "%");

                        Thread.sleep(5000);

                        reportFW.flush();
                    }

                    serverService.stop();

                    serverService.rollbackPreviousSet();

                    logger.info("Server Down");

                    write("</tr>");
                }
                write("</table>");

                File imageFile = new File(reportFile.getName() + "." + +papIndex + "." + pd.name + ".jpeg");

                imageFile.createNewFile();

                //output matrix
                GraphHelper.exportToImage(imageFile, pd, reportMatrix);

                write("<img src=\"" + imageFile.getName() + "\"/>");

            }

            reportFW.flush();
            reportFW.close();

            logger.info("Success & EXIT !! Please Look @ Report File:"
                    + reportFile.getAbsolutePath());

        } catch (Throwable e) {
            clientService.stop();
            serverService.stop();
            throw e;
        }

    }

    public static Service createService(String url)
            throws MalformedURLException {
        HessianProxyFactory factory = new HessianProxyFactory();
        Service service = (Service) factory.create(Service.class, url);
        return service;
    }

}
