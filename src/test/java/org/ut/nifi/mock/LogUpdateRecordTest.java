package org.ut.nifi.mock;

import java.io.File;

import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.annotations.NiFiEntity;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;
import org.ut.nifi.tools.NiFiTestTools;

public class LogUpdateRecordTest {
	private static final Logger logger = LoggerFactory.getLogger(LogUpdateRecordTest.class);
	
	private static final String TEMPLATE_NAME = "log-template.xml";
	
	private static final String LOGS_EVENT = "[{\"date\":\"18/12/14 14:40:03\",\"severity\":\"INFO\",\"class\":\"executor.CoarseGrainedExecutorBackend\",\"data\":\"Started daemon with process name: 9384@dhadlx120\",\"irt\":\"tst\"},{\"date\":\"18/12/14 14:45:23\",\"severity\":\"INFO\",\"class\":\"util.ShutdownHookManager\",\"data\":\"Shutdown hook called\",\"irt\":\"tst\"}]";
	private static final String EXPECTED_LOGS_EVENT = "[{\"date\":1544794803000,\"severity\":\"INFO\",\"class\":\"executor.CoarseGrainedExecutorBackend\",\"data\":\"Started daemon with process name: 9384@dhadlx120\",\"irt\":\"tst\"},{\"date\":1544795123000,\"severity\":\"INFO\",\"class\":\"util.ShutdownHookManager\",\"data\":\"Shutdown hook called\",\"irt\":\"tst\"}]";
	
	@NiFiEntity(name = "LogUpdateRecord")
	TestRunner logUpdateRecord;

	@Before
	public void setup() throws NiFiMockInitException {
		String templateFilePath = this.getClass().getClassLoader().getResource(TEMPLATE_NAME).getPath();
		NiFiMock.init(new File(templateFilePath), this);
		logger.info("template initialized");
	}
	
	@Test
	public void logUpdateRecord_should_return_event_with_timestamp_format() {
		// GIVEN
		logUpdateRecord.enqueue(LOGS_EVENT);
		// WHEN
		logUpdateRecord.run(1);
		// THEN
		NiFiTestTools.getFirstSuccessFlowFile(logUpdateRecord).assertContentEquals(EXPECTED_LOGS_EVENT);
	}
}
