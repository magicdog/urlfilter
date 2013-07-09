package com.hipu.bdb.tair;

import java.net.URL;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.httpclient.URIException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.sleepycat.je.DatabaseException;

public class TairTest {
	private static final Logger logger = Logger.getLogger(TairTest.class);
	
	private final String CHAR_SET = "ABCDEFGHIJKLMNOPQISTUVWXYZabcdefghijklmnopqistuvwxyz123456789.";
	private final int CHAR_LENGTH = CHAR_SET.length();
	private final String PREFIX = "http://";
	private final int URL_LENGTH = 107;
	
//	private ExecutorService service;
	private ThreadPoolExecutor service;
	
	public static AtomicLong count = new AtomicLong(0);
	
	public static AtomicLong start = new AtomicLong(0);
	
	@Parameter(description = "urlCount", names = "-u")
	private long urlCount;
	@Parameter(description = "threadCount", names = "-t")
	private int threadCount;
	@Parameter(description = "sleepTime", names = "-s")
	private int sleepTime;

	private TairFilter filter = null;

	protected TairTest() throws Exception {
		this.filter = new TairFilter();
		
	}

	protected void tearDown() throws Exception {
		this.filter.close();
	}
	
	class FilterJob implements Runnable {
		
		private TairFilter filter;
		
		private String url;
		
		public FilterJob(TairFilter f, String url) {
			this.filter = f;
			this.url = url;
		}

		@Override
		public void run() {
			if (!this.filter.contains(url)) {
//				this.filter.add(url);
			}
			long count = TairTest.count.incrementAndGet();
			long start = TairTest.start.get();
			if (count > 0 && ((count % 1000) == 0)) {
				logger.info("Processed "
						+ count
						+ " in "
						+ (System.currentTimeMillis() - start)
						+ " task count "
						+ service.getMaximumPoolSize());
				
				TairTest.start.set(System.currentTimeMillis());
			}
			return;
		}
	}

	protected void testWriting() throws DatabaseException, URIException {
		start.set(System.currentTimeMillis());
		long start = System.currentTimeMillis();
		service  = new ThreadPoolExecutor(this.threadCount, Integer.MAX_VALUE, 20, TimeUnit.SECONDS, new LinkedBlockingQueue());
//		service = Executors.newFixedThreadPool(this.threadCount);
		Random random = new Random(System.currentTimeMillis());
		long count = 0;
		int urlLength;
		StringBuffer url = new StringBuffer(PREFIX);
		for (; count < this.urlCount; count++) {
			urlLength = random.nextInt(URL_LENGTH);
			for (int j = 0; j < urlLength; j++) {
				url.append(CHAR_SET.charAt(random.nextInt(CHAR_LENGTH)));
			}
			FilterJob job = new FilterJob(this.filter, url.toString());
			service.execute(job);
			// logger.info(url.toString());
			url.delete(PREFIX.length(), url.length());
			if (count >0 && count % 1000000 == 0)
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		service.shutdown();
		while (!service.isTerminated()) {}
		
			logger.info("Processed " + count + " in "
				+ (System.currentTimeMillis() - start));
	}
	
	

	public static void main(String args[]) throws Exception {
		URL url = TairTest.class.getResource("/log4j.properties");
		if (url == null) {
			url = TairTest.class.getResource("/conf/log4j.properties");
		}
		PropertyConfigurator.configure(url);
		TairTest test = new TairTest();
		JCommander commander = new JCommander(test);
		try {
			commander.parse(args);
		} catch (ParameterException e) {
			logger.error(e.getMessage());
			commander.usage();
		}
		test.testWriting();
		test.tearDown();
	}

}
