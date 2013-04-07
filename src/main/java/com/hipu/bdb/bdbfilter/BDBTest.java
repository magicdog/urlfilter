package com.hipu.bdb.bdbfilter;

import java.io.File;
import java.net.URL;
import java.util.Random;

import org.apache.commons.httpclient.URIException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.sleepycat.je.DatabaseException;

public class BDBTest {
	private static final Logger logger = Logger.getLogger(BDBTest.class);

	private final String CHAR_SET = "abcdefghijklmnopqistuvwxyz123456.";
	private final int CHAR_LENGTH = CHAR_SET.length();
	private final String PREFIX = "http://";
	private final int URL_LENGTH = 107;
	@Parameter(description = "urlCount", names = "-u")
	private long urlCount;
	@Parameter(description = "BDB Dir", names = "-d")
	private String dbDir;

	private BdbUriUniqFilter filter = null;
	private File bdbDir = null;

	protected BDBTest() throws Exception {
		// Remove any bdb that already exists.
		this.bdbDir = new File(this.dbDir, this.getClass().getName());
		System.out.println("db file path: " + this.bdbDir.getAbsolutePath());
		// if (this.bdbDir.exists()) {
		// org.apache.commons.io.FileUtils.deleteDirectory(bdbDir);
		// }
		// dbdir, cacheSizePercentage
		this.filter = new BdbUriUniqFilter(bdbDir, 50);
	}

	protected void tearDown() throws Exception {
		this.filter.close();
	}

	protected void testWriting() throws DatabaseException, URIException {
		long start = System.currentTimeMillis();
		Random random = new Random(System.currentTimeMillis());
		long count = 0;
		int urlLength;
		StringBuffer url = new StringBuffer(PREFIX);
		for (; count < this.urlCount; count++) {
			urlLength = random.nextInt(URL_LENGTH);
			for (int j = 0; j < urlLength; j++) {
				url.append(CHAR_SET.charAt(random.nextInt(CHAR_LENGTH)));
			}
			this.filter.setAdd(url.toString());
			// logger.info(url.toString());
			url.delete(PREFIX.length(), url.length());
			if (count > 0 && ((count % 10000) == 0)) {
				this.logger.info("Added "
						+ count
						+ " in "
						+ (System.currentTimeMillis() - start)
						+ " misses "
						+ ((BdbUriUniqFilter) this.filter).getCacheMisses()
						+ " diff of misses "
						+ ((BdbUriUniqFilter) this.filter)
								.getLastCacheMissDiff());
				start = System.currentTimeMillis();
			}
		}
		this.logger.info("Added " + count + " in "
				+ (System.currentTimeMillis() - start));
	}

	public static void main(String args[]) throws Exception {
		URL url = BDBTest.class.getResource("/log4j.properties");
		if (url == null) {
			url = BDBTest.class.getResource("/conf/log4j.properties");
		}
		PropertyConfigurator.configure(url);
		BDBTest test = new BDBTest();
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
