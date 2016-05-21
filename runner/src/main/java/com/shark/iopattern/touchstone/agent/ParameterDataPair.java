package com.shark.iopattern.touchstone.agent;

import java.util.List;
import java.util.Map;

public class ParameterDataPair{
	public String name;
	public Map<String, String> initServerParameters;
	public Map<String, String> initClientParameters;
	public List<Map<String, String>> serverParameters;
	public List<Map<String, String>> clientParameters;
}