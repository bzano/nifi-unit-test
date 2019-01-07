package org.ut.nifi.mock;

import java.io.File;

import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.annotations.NiFiEntity;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class PutHDFSTest {
	private static final Logger logger = LoggerFactory.getLogger(LogUpdateRecordTest.class);
	
	private static final String TEMPLATE_NAME = "hdfs-template.xml";
	
	@NiFiEntity(name = "PutHDFS")
	TestRunner putHDFS;
	
	@Before
	public void setup() throws NiFiMockInitException {
		String templateFilePath = this.getClass().getClassLoader().getResource(TEMPLATE_NAME).getPath();
		try {
			NiFiMock.init(new File(templateFilePath), this);
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		logger.info("template initialized");
	}
	
	@Test
	public void puthdfs_test() {
		// GIVEN
		// WHEN
		//putHDFS.run();
		// THEN
		logger.warn("APP LAUNCHED");
	}
}
