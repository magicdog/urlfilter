package com.hipu.bdb.tair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

public class Tair {
	
	private static final Logger LOG = Logger.getLogger(Tair.class);
	
	private DefaultTairManager tairManager;
	
	public Tair(List<String> confServers, String groupName) {
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);
		tairManager.setGroupName(groupName);
		tairManager.setTimeout(20000);
//		tairManager.setMaxWaitThread(200);
		tairManager.init();
	}
	
	
	public String get(int space, String key) {
		Result<DataEntry> results = tairManager.get(space, key);
		if (results.isSuccess()) {
			DataEntry data = results.getValue();
			if (data != null)
				return (String) data.getValue();
//			else 
//				LOG.error("can not get the value by the key "+key+" in namespace "+space);
		}
//		else {
//			LOG.error("can not get the value by the key "+key+" in namespace "+space);
//		}
		return null;
	}
	
	public Map<String,String> batchGet(int space, List<String> key) {
		Result<List<DataEntry>> results = tairManager.mget(space, key);
		Map<String, String> result = new HashMap<String, String>();
		if (results.isSuccess()) {
			List<DataEntry> data = results.getValue();
			for (DataEntry entry : data) {
				if (entry != null)
					result.put((String)entry.getKey(), (String)entry.getValue());
			}
		}
		else {
			LOG.error("can not get the value by the batch key in namespace "+space);
		}
		return result;
	}
	
	//0 success
	public int put(int space, String key, String value) {
		ResultCode rc = tairManager.put(space, key, value);
		if (!rc.isSuccess())
		{
			LOG.error("put ( "+key+" , "+value+" ) into namespace "+ space +" error!");
		}
		return rc.getCode();
	}
	
	public int put(int space, String key, String value, int version) {
		ResultCode rc = tairManager.put(space, key, value, version);
		if (!rc.isSuccess())
		{
			LOG.error("put ( "+key+" , "+value+" ) into namespace "+ space +" error!");
		}
		return rc.getCode();
	}
	
	public int put(int space, String key, String value, int version, int expireTime) {
		ResultCode rc = tairManager.put(space, key, value, version, expireTime);
		if (!rc.isSuccess())
		{
			LOG.error("put ( "+key+" , "+value+" ) into namespace "+ space +" error!");
		}
		return rc.getCode();
	}
	
	public int delete(int space, String key) {
		ResultCode rc = tairManager.delete(space, key);
		if (!rc.isSuccess())
		{
			LOG.error("delete "+key+" in namespace "+ space +"error!");
		}
		return rc.getCode();
	}
	
	
	public void close() {
		tairManager.close();
	}
	
	
	
	public static void main(String args[]) {
		List<String> confServers = new ArrayList<String>();
		confServers.add("192.168.68.13:5198");
		
		Tair t = new Tair(confServers, "group_1");
		t.put(12, "url", "cotnent");
		t.get(12, "url");
	}
	
}

