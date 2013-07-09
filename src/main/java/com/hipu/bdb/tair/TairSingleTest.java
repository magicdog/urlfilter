package com.hipu.bdb.tair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.httpclient.URIException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.sleepycat.je.DatabaseException;

public class TairSingleTest {
	private static final Logger logger = Logger.getLogger(TairSingleTest.class);
	
	private final String CHAR_SET = "abcdefghijklmnopqistuvwxyz123456789.";
	private final int CHAR_LENGTH = CHAR_SET.length();
	private final String PREFIX = "http://";
	private final int URL_LENGTH = 107;
	
	public long count;
	
	public long totaltime;
	
	@Parameter(description = "urlCount", names = "-u")
	private long urlCount;

	private TairFilter filter = null;

	protected TairSingleTest() throws Exception {
		this.filter = new TairFilter();
	}

	protected void tearDown() throws Exception {
		this.filter.close();
	}
	
	protected void testWrite() {
		Random random = new Random(System.currentTimeMillis());
		int urlLength;
		long hit = 0;
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File("urls")));
		
		StringBuffer url = new StringBuffer(PREFIX);
		while(true) {
			urlLength = random.nextInt(URL_LENGTH);
			
			for (int j = 0; j < urlLength; j++) {
				url.append(CHAR_SET.charAt(random.nextInt(CHAR_LENGTH)));
			}
			this.filter.add(url.toString());
			hit++;
			System.out.println(hit);
			writer.write(url.toString()+"\n");
				if (hit >= this.urlCount)
					break;
			url.delete(PREFIX.length(), url.length());
		}
		writer.close();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return;
	}
	
	protected void testRandomWrite() {
		Random random = new Random(System.currentTimeMillis());
		int urlLength;
		long hit = 0;
		StringBuffer url = new StringBuffer(PREFIX);
		while(true) {
			urlLength = random.nextInt(URL_LENGTH);
			
			for (int j = 0; j < urlLength; j++) {
				url.append(CHAR_SET.charAt(random.nextInt(CHAR_LENGTH)));
			}
			this.filter.add(url.toString());
			hit++;
				if (hit >= this.urlCount)
					break;
			url.delete(PREFIX.length(), url.length());
		}
		
		return;
	}
	
	protected void testSingleRead(String key) throws DatabaseException, URIException {
		System.out.println(this.filter.contains(key));
	}
	
	protected void testRandomRead() throws DatabaseException, IOException {
		long maxtime = 0;
		long mintime = -1;
		long diff2 = 0;
		long hit = 0;
		Random random = new Random(System.currentTimeMillis());
		int urlLength;
		StringBuffer url = new StringBuffer(PREFIX);
//		BufferedWriter writer = new BufferedWriter(new FileWriter(new File("hit")));
		while(true){
			urlLength = random.nextInt(URL_LENGTH);
			
			for (int j = 0; j < urlLength; j++) {
				url.append(CHAR_SET.charAt(random.nextInt(CHAR_LENGTH)));
			}
			
			long start = System.currentTimeMillis();

//			System.out.println(url.toString());
			if (this.filter.contains(url.toString())) {
//				writer.write(url.toString()+"\n");
				long diff = System.currentTimeMillis() - start;
				hit++;
				diff2 += diff; 
				if (mintime == -1 || diff < mintime)
					mintime = diff;
				else if (diff > maxtime)
					maxtime = diff;
				System.out.println(url.toString() + " " + diff);
				if ((hit % 1000) == 0) {
					logger.info("Processed " + hit + " in " + diff2
							+ " max " + maxtime + " min " + mintime);
					totaltime += diff2;
					maxtime = 0;
					mintime = -1;
					diff2 = 0;
				}
				if (hit >= this.urlCount)
					break;
			}
			url.delete(PREFIX.length(), url.length());
		}
//		writer.close();
		return;
	}
	
	protected void testRead() throws DatabaseException, URIException {
		long maxtime = 0;
		long mintime = -1;
		long diff2 = 0;
		long hit = 0;
		List<String> lists = new ArrayList<String>();
		Set<Integer> sets = new HashSet<Integer>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					"dump.1")));
			String line;
			while ((line = reader.readLine()) != null)
				lists.add(line);
			
			System.out.println(lists.size());
			int size = lists.size();
			Random random = new Random(System.currentTimeMillis());
			for (int i=0; i<100000;i++) {
				int index = random.nextInt(size-1);
				String url = lists.get(index);
				long start = System.currentTimeMillis();
				if (this.filter.contains(url)) {   
					long diff = System.currentTimeMillis() - start;
//					System.out.println(url);
					hit++;
					diff2 += diff;
					if (mintime == -1 || diff < mintime)
						mintime = diff;
					else if (diff > maxtime)
						maxtime = diff;
//					System.out.println(str + " " + diff);
					if ((hit % 1000) == 0) {
						logger.info("Processed " + hit + " in " + diff2
								+ " max " + maxtime + " min " + mintime);
						totaltime += diff2;
						maxtime = 0;
						mintime = -1;
						diff2 = 0;
					}
				}
			}
			
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return;
	}
	
	

	public static void main(String args[]) throws Exception {
		URL url = TairSingleTest.class.getResource("/log4j.properties");
		if (url == null) {
			url = TairSingleTest.class.getResource("/conf/log4j.properties");
		}
		PropertyConfigurator.configure(url);
		TairSingleTest test = new TairSingleTest();
		JCommander commander = new JCommander(test);
		try {
			commander.parse(args);
		} catch (ParameterException e) {
			logger.error(e.getMessage());
			commander.usage();
		}
//		test.testSingleRead("http://qmbuemx6xpma");
//		test.testRandomRead();
		test.testRead();
		test.tearDown();
		return;
	}

}
