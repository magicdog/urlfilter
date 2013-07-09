package com.hipu.bdb.tair;

import java.util.ArrayList;
import java.util.List;

public class TairFilter {

	private Tair tair;
	
	public TairFilter() {
		List<String> confServers = new ArrayList<String>();
		confServers.add("192.168.68.13:5198");
		tair = new Tair(confServers, "group_1");
	}
	
	public boolean contains(String key) {
		return tair.get(0, key) != null;
	}
	
	public boolean add(String key) {
		return tair.put(0, key, "") == 0;
	}
	
	public void close() {
		tair.close();
	}
	
	public static void main(String[] args) {
		TairFilter df = new TairFilter();
		
	}
	
}
