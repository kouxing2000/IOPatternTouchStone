/**
 * 
 */
package com.shark.iopattern.touchstone.agent;

import java.awt.Dimension;
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.swing.JPanel;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.LegendItemCollection;
import org.jfree.chart.axis.AxisSpace;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.StandardCategoryToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.CombinedDomainCategoryPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;


/**
 * @author weili1
 * 
 */
public class GraphHelper {

	public static void exportToImage(File imageFile, ParameterDataPair pd,
			IndexItem[][] reportMatrix) throws IOException {
		JFreeChart chart = createChart(pd, reportMatrix);
		int width = 40 * reportMatrix.length * reportMatrix[0].length;
		ChartUtilities.saveChartAsJPEG(imageFile, chart, width > 600 ? width : 600, 1000);
	}
	
	private static interface ValueGetter{
		double get(IndexItem item);
	}

	private static CategoryDataset createDataset(
			ParameterDataPair pd, IndexItem[][] reportMatrix, ValueGetter vg) {
		DefaultCategoryDataset defaultcategorydataset = new DefaultCategoryDataset();

		for (int i = 0; i < reportMatrix.length; i++) {
			for (int j = 0; j < reportMatrix[i].length; j++) {
				defaultcategorydataset.addValue(vg.get(reportMatrix[i][j]),
						pd.serverParameters.get(i).toString().replace("com.shark.iopattern.touchstone", ""),
						pd.clientParameters.get(j).toString().replace("com.shark.iopattern.touchstone", ""));
			}
		}

		return defaultcategorydataset;
	}
	
	private static CategoryPlot createCategoryPlot(BarRenderer barrenderer, String description, ParameterDataPair pd,
			IndexItem[][] reportMatrix, ValueGetter vg){
		CategoryDataset categorydataset = createDataset(
				pd, reportMatrix, vg);
		NumberAxis numberaxis = new NumberAxis(description);
		numberaxis
				.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		
		//numberaxis.setLabelFont(new Font("Tohoma", Font.PLAIN,
		//		10));

		CategoryPlot categoryplot = new CategoryPlot(
				categorydataset, null, numberaxis, barrenderer);
		categoryplot.setDomainGridlinesVisible(false);
		
		return categoryplot;
	}

	private static JFreeChart createChart(ParameterDataPair pd,
			IndexItem[][] reportMatrix) {

		CategoryAxis categoryaxis = new CategoryAxis("Client Modes");
		
		categoryaxis.setMaximumCategoryLabelLines(100);
		
		CombinedDomainCategoryPlot combineddomaincategoryplot = new CombinedDomainCategoryPlot(
				categoryaxis);

		BarRenderer barrenderer = new BarRenderer();
		barrenderer
				.setBaseToolTipGenerator(new StandardCategoryToolTipGenerator());

		int indexOfPlot = 1;
		{
			CategoryPlot  categoryplot = createCategoryPlot(barrenderer, "throughput(s)", pd, reportMatrix, new ValueGetter(){
				@Override
				public double get(IndexItem item) {
					return item.getThroughput();
				}
			});
			categoryplot.setFixedRangeAxisSpace(new AxisSpace());
			if (indexOfPlot++ > 1){
				categoryplot.setFixedLegendItems(new LegendItemCollection());
			}
			combineddomaincategoryplot.add(categoryplot, 100);
		}
		
		{
			CategoryPlot  categoryplot = createCategoryPlot(barrenderer, "AVG Latency(ms)", pd, reportMatrix, new ValueGetter(){
				@Override
				public double get(IndexItem item) {
					return item.getAverageLatency();
				}
			});
			if (indexOfPlot++ > 1){
				categoryplot.setFixedLegendItems(new LegendItemCollection());
			}
			combineddomaincategoryplot.add(categoryplot, 60);
		}
		
		{
			CategoryPlot  categoryplot = createCategoryPlot(barrenderer, "Most<Latency(ms)", pd, reportMatrix, new ValueGetter(){
				@Override
				public double get(IndexItem item) {
					return item.getMostBelowLatency();
				}
			});
			if (indexOfPlot++ > 1){
				categoryplot.setFixedLegendItems(new LegendItemCollection());
			}
			combineddomaincategoryplot.add(categoryplot, 60);
		}
		
		{
			CategoryPlot  categoryplot = createCategoryPlot(barrenderer, "S CPU(%)", pd, reportMatrix, new ValueGetter(){
				@Override
				public double get(IndexItem item) {
					return item.getServerCPUUsage() * 100;
				}
			});
			if (indexOfPlot++ > 1){
				categoryplot.setFixedLegendItems(new LegendItemCollection());
			}
			combineddomaincategoryplot.add(categoryplot, 40);
		}
		
		{
			CategoryPlot  categoryplot = createCategoryPlot(barrenderer, "C CPU(%)", pd, reportMatrix, new ValueGetter(){
				@Override
				public double get(IndexItem item) {
					return item.getClientCPUUsage() * 100;
				}
			});
			if (indexOfPlot++ > 1){
				categoryplot.setFixedLegendItems(new LegendItemCollection());
			}
			combineddomaincategoryplot.add(categoryplot, 40);
		}
		
		
		JFreeChart jfreechart = new JFreeChart("Test Result Matrix", new Font("SansSerif", Font.PLAIN,
				12), combineddomaincategoryplot, true);
		
		ChartUtilities.applyCurrentTheme(jfreechart);

		return jfreechart;
	}

	public static void main(String args[]) {
		String s = "Demo";

		ParameterDataPair pd = new ParameterDataPair();

		pd.serverParameters = new ArrayList<Map<String, String>>();
		for (int i = 0; i < 1; i++) {
			pd.serverParameters.add(new HashMap<String, String>());
			pd.serverParameters.get(i).put("A", "" + i);
		}

		pd.clientParameters = new ArrayList<Map<String, String>>();
		for (int i = 0; i < 8; i++) {
			pd.clientParameters.add(new HashMap<String, String>());
			pd.clientParameters.get(i).put("B", "" + i);
		}

		IndexItem[][] reportMatrix = new IndexItem[pd.serverParameters.size()][];
		
		Random random = new Random();
		
		for (int i = 0; i < reportMatrix.length; i++) {
			reportMatrix[i] = new IndexItem[pd.clientParameters.size()];
			for (int j = 0; j < reportMatrix[i].length; j++) {
				reportMatrix[i][j] = new IndexItem();
				reportMatrix[i][j].setAverageLatency(random.nextDouble() * 100);
				reportMatrix[i][j].setMostBelowLatency(random.nextDouble() * 100);
				reportMatrix[i][j].setThroughput(random.nextInt(1000) + 5000);
				reportMatrix[i][j].setServerCPUUsage(random.nextDouble());
				reportMatrix[i][j].setClientCPUUsage(random.nextDouble());
			}
		}

		JFreeChart jfreechart = createChart(pd, reportMatrix);
		JPanel jpanel = new ChartPanel(jfreechart);
		int width = 40 * reportMatrix.length * reportMatrix[0].length;
		jpanel.setPreferredSize(new Dimension(width > 600 ? width : 600, 1000));
		ApplicationFrame af = new ApplicationFrame(s);
		af.setContentPane(jpanel);
		af.pack();
		RefineryUtilities.centerFrameOnScreen(af);
		af.setVisible(true);
	}
}
