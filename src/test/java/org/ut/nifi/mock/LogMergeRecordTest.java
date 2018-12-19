package org.ut.nifi.mock;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.annotations.NiFiEntity;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class LogMergeRecordTest {
	private static final Logger logger = LoggerFactory.getLogger(LogMergeRecordTest.class);

	private static final String TEMPLATE_NAME = "log-template.xml";
	
	private static final String START_DAEMON_EVENT = "18/12/14 14:40:03 INFO executor.CoarseGrainedExecutorBackend: Started daemon with process name: 9384@dhadlx120";
	private static final String RUNNING_LOG_EVENT = "18/12/14 14:43:50 INFO org.apache.spark.Runner: Application report for application_1460098549233_0013 (state: RUNNING)";
	private static final String SHUTDOWN_HOOK_EVENT = "18/12/14 14:45:23 INFO util.ShutdownHookManager: Shutdown hook called";
	
	private static final String EXPECTED_MERGED_EVENT = "[{\"date\":\"18/12/14 14:40:03\",\"severity\":\"INFO\",\"class\":\"executor.CoarseGrainedExecutorBackend\",\"data\":\"Started daemon with process name: 9384@dhadlx120\",\"irt\":\"tst\"},{\"date\":\"18/12/14 14:45:23\",\"severity\":\"INFO\",\"class\":\"util.ShutdownHookManager\",\"data\":\"Shutdown hook called\",\"irt\":\"tst\"}]";
	
	private static final String MERGED_ROUTE = "merged";
	
	@NiFiEntity(name = "LogMergeRecord")
	TestRunner logMergeRecord;

	@Before
	public void setup() throws NiFiMockInitException {
		String templateFilePath = this.getClass().getClassLoader().getResource(TEMPLATE_NAME).getPath();
		NiFiMock.init(new File(templateFilePath), this);
		logger.info("template initialized");
	}
	
	@Test
	public void logMergeRecord_should_merge_two_flow() {
		// GIVEN
		logMergeRecord.enqueue(START_DAEMON_EVENT);
		logMergeRecord.enqueue(SHUTDOWN_HOOK_EVENT);
		// WHEN
		logMergeRecord.run(1);
		// THEN
		logMergeRecord.getFlowFilesForRelationship(MERGED_ROUTE).get(0).assertContentEquals(EXPECTED_MERGED_EVENT);
	}
	
	@Test
	public void logMergeRecord_should_not_merge_less_then_two_flow() {
		// GIVEN
		logMergeRecord.enqueue(START_DAEMON_EVENT);
		// WHEN
		logMergeRecord.run(1);
		// THEN
		assertEquals("messages in merged route should be empty", logMergeRecord.getFlowFilesForRelationship(MERGED_ROUTE).size(), 0);
	}

	@Test
	public void logMergeRecord_should_always_merge_two_flow() {
		// GIVEN
		logMergeRecord.enqueue(START_DAEMON_EVENT);
		logMergeRecord.enqueue(SHUTDOWN_HOOK_EVENT);
		logMergeRecord.enqueue(RUNNING_LOG_EVENT);
		// WHEN
		logMergeRecord.run(1);
		// THEN
		logMergeRecord.getFlowFilesForRelationship(MERGED_ROUTE).get(0).assertContentEquals(EXPECTED_MERGED_EVENT);
	}
}
