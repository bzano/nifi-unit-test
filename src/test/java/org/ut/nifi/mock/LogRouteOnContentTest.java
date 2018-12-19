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

public class LogRouteOnContentTest {
	private static final Logger logger = LoggerFactory.getLogger(LogRouteOnContentTest.class);

	private static final String TEMPLATE_NAME = "log-template.xml";
	
	private static final String START_DAEMON_EVENT = "18/12/14 14:40:03 INFO executor.CoarseGrainedExecutorBackend: Started daemon with process name: 9384@dhadlx120";
	private static final String SHUTDOWN_HOOK_EVENT = "18/12/14 14:45:23 INFO util.ShutdownHookManager: Shutdown hook called";
	private static final String RUNNING_LOG_EVENT = "13/04/18 15:09:50 INFO org.apache.spark.Runner: Application report for application_1460098549233_0013 (state: RUNNING)";
	
	private static final String END_ROUTE = "job-end";
	private static final String START_ROUTE = "job-start";
	private static final String UNMATCHED_ROUTE = "unmatched";
	
	@NiFiEntity(name = "LogRouteOnContent")
	TestRunner logRouteOnContent;
	
	@Before
	public void setup() throws NiFiMockInitException {
		String templateFilePath = this.getClass().getClassLoader().getResource(TEMPLATE_NAME).getPath();
		NiFiMock.init(new File(templateFilePath), this);
		logger.info("template initialized");
	}
	
	@Test
	public void logRouteOnContent_should_route_to_log_unmached_when_it_is_a_random_log() {
		// GIVEN
		logRouteOnContent.enqueue(RUNNING_LOG_EVENT);
		// WHEN
		logRouteOnContent.run(1);
		// THEN
		assertEquals("messages in unmatched route should not be empty", logRouteOnContent.getFlowFilesForRelationship(UNMATCHED_ROUTE).isEmpty(), false);
	}
	
	@Test
	public void logRouteOnContent_should_route_to_log_start_when_it_is_a_start_log() {
		// GIVEN
		logRouteOnContent.enqueue(START_DAEMON_EVENT);
		// WHEN
		logRouteOnContent.run(1);
		// THEN
		assertEquals("messages in start route should not be empty", logRouteOnContent.getFlowFilesForRelationship(START_ROUTE).isEmpty(), false);
	}
	
	@Test
	public void logRouteOnContent_should_route_to_log_end_when_it_is_an_end_log() {
		// GIVEN
		logRouteOnContent.enqueue(SHUTDOWN_HOOK_EVENT);
		// WHEN
		logRouteOnContent.run(1);
		// THEN
		assertEquals("messages in end route should not be empty", logRouteOnContent.getFlowFilesForRelationship(END_ROUTE).isEmpty(), false);
	}
}
