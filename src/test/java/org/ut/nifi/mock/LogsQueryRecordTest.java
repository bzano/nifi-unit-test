package org.ut.nifi.mock;

import java.io.File;

import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.annotations.NiFiEntity;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class LogsQueryRecordTest {
	private static final Logger logger = LoggerFactory.getLogger(LogsQueryRecordTest.class);

	private static final String TEMPLATE_NAME = "log-template.xml";
	
	private static final String MERGED_EVENT = "[{\"date\":\"0\",\"severity\":\"INFO\",\"class\":\"executor.CoarseGrainedExecutorBackend\",\"data\":\"Started daemon with process name: 9384@dhadlx120\",\"irt\":\"tst\"},{\"date\":\"50\",\"severity\":\"INFO\",\"class\":\"util.ShutdownHookManager\",\"data\":\"Shutdown hook called\",\"irt\":\"tst\"}]";
	
	private static final String EXPECTED_MERGED_EVENT = "[{\"ts_start\":0,\"ts_end\":50,\"duration\":50,\"severity\":\"INFO\",\"data\":\"Job success\",\"irt\":\"tst\"}]";
	
	private static final String MERGED_ROUTE = "merged";
	
	@NiFiEntity(name = "LogsQueryRecord")
	TestRunner logsQueryRecord;
	
	@Before
	public void setup() throws NiFiMockInitException {
		String templateFilePath = this.getClass().getClassLoader().getResource(TEMPLATE_NAME).getPath();
		NiFiMock.init(new File(templateFilePath), this);
		logger.info("template initialized");
	}
	
	@Test
	public void logsQueryRecord_should_return_merged_event() {
		// GIVEN
		logsQueryRecord.enqueue(MERGED_EVENT);
		// WHEN
		logsQueryRecord.run(1, true, false);
		// THEN
		logsQueryRecord.getFlowFilesForRelationship(MERGED_ROUTE).get(0).assertContentEquals(EXPECTED_MERGED_EVENT);
	}
}
