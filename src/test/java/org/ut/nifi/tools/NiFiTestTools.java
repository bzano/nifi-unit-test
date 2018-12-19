package org.ut.nifi.tools;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NiFiTestTools {
	private static final Logger logger = LoggerFactory.getLogger(NiFiTestTools.class);
	
	private static final String FAILURE = "failure";
	private static final String SUCCESS = "success";
	
	public static void sendMessage(String topic, Map<String, Object> producerProps, String message) {
		try(KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)){
			Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>(topic, message));
			result.get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public static void runKafkaConsumer(TestRunner runner, int times) {
		for(int i = 0; i < times; i++) {
			runner.run(1, false);
		}
		runner.run(1, true);
	}
	
	public static MockFlowFile getFirstSuccessFlowFile(TestRunner runner) {
		return runner.getFlowFilesForRelationship(SUCCESS).get(0);
	}
	
	public static int getSuccessFlowFilesSize(TestRunner runner) {
		return runner.getFlowFilesForRelationship(SUCCESS).size();
	}
	
	public static int getFailureFlowFilesSize(TestRunner runner) {
		return runner.getFlowFilesForRelationship(FAILURE).size();
	}
	
	public static MockFlowFile getFirstFailureFlowFile(TestRunner runner) {
		return runner.getFlowFilesForRelationship(FAILURE).get(0);
	}
}
