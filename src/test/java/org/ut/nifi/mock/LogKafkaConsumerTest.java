package org.ut.nifi.mock;

import java.io.File;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.ut.nifi.mock.annotations.NiFiEntity;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;
import org.ut.nifi.tools.NiFiTestTools;

public class LogKafkaConsumerTest {
	private static final Logger logger = LoggerFactory.getLogger(LogKafkaConsumerTest.class);

	private static final String TOPIC = "tpc-1";
	private static final String TEMPLATE_NAME = "log-template.xml";
	
	private static final String RUNNING_LOG_EVENT = "13/04/18 15:09:50 INFO org.apache.spark.Runner: Application report for application_1460098549233_0013 (state: RUNNING)";
	
	@NiFiEntity(name = "LogKafkaConsumer")
	TestRunner logKafkaConsumer;
	
	@ClassRule
	public final static EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(1, true, TOPIC).kafkaPorts(9092);
	private Map<String, Object> producerProps;
	
	@Before
	public void setup() throws NiFiMockInitException {
		String templateFilePath = this.getClass().getClassLoader().getResource(TEMPLATE_NAME).getPath();
		NiFiMock.init(new File(templateFilePath), this);
		logger.info("template initialized");
		
		producerProps = KafkaTestUtils.producerProps(kafka.getEmbeddedKafka());
		
		logKafkaConsumer.setProperty("auto.offset.reset", "earliest");
	}
	
	@Test
	public void logKafkaConsumer_should_read_record_from_kafka() {
		// GIVEN
		NiFiTestTools.sendMessage(TOPIC, producerProps, RUNNING_LOG_EVENT);
		// WHEN
		NiFiTestTools.runKafkaConsumer(logKafkaConsumer, 10);
		// THEN
		NiFiTestTools.getFirstSuccessFlowFile(logKafkaConsumer);
	}
	
	@Test
	public void logKafkaConsumer_should_read_same_message() {
		// GIVEN
		NiFiTestTools.sendMessage(TOPIC, producerProps, RUNNING_LOG_EVENT);
		// WHEN
		NiFiTestTools.runKafkaConsumer(logKafkaConsumer, 10);
		// THEN
		NiFiTestTools.getFirstSuccessFlowFile(logKafkaConsumer).assertContentEquals(RUNNING_LOG_EVENT);
	}
}
