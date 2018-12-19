package org.ut.nifi.mock;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_0_10;
import org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_0_10;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.annotations.NiFiEntity;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class NiFiMockProperty {
	private static final Logger logger = LoggerFactory.getLogger(NiFiMockProperty.class);

	private FlowController flowController;
	private TemplateDTO template;
	private Object caller;

	protected NiFiMockProperty(FlowController flowController, TemplateDTO template, Object caller) {
		this.flowController = flowController;
		this.template = template;
		this.caller = caller;
	}

	protected void updateFieldsProcessor() throws NiFiMockInitException {
		Arrays.asList(caller.getClass().getDeclaredFields()).stream()
				.filter(field -> field.getAnnotation(NiFiEntity.class) != null)
				.filter(field -> TestRunner.class.isAssignableFrom(field.getType()))
				.forEach(this::buildProcessorForField);
	}

	private void buildProcessorForField(Field field) {
		Optional<TestRunner> processor = getProcessor(field);

		if (processor.isPresent()) {
			modifyFieldValue(field, processor.get());
		} else {
			logger.error("Processor for field " + field.getName() + " is not found");
		}
	}

	private Optional<TestRunner> getProcessor(Field field) {
		if (logger.isDebugEnabled()) {
			logger.debug("Get processor for field " + field.getName());
		}
		NiFiEntity annotation = field.getAnnotation(NiFiEntity.class);
		String processorName = annotation.name();
		Class<?> processorType = annotation.type();
		if (StringUtils.isEmpty(processorName)) {
			return getProcessorByType(processorType);
		}
		return getProcessorByName(processorName);
	}

	private void modifyFieldValue(Field field, TestRunner newProcessor) {
		logger.debug("Set new value for field " + field.getName());
		try {
			field.setAccessible(true);
			field.set(caller, newProcessor);
			field.setAccessible(false);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			logger.error("Error while updating field " + field.getName(), e);
		}
	}

	private Optional<TestRunner> getProcessorByType(Class<?> fieldType) {
		if (logger.isDebugEnabled()) {
			logger.debug("Get processor for type " + fieldType.getName());
		}
		return template.getSnippet().getProcessors().stream()
				.filter(processorDTO -> processorDTO.getType().equals(fieldType.getCanonicalName())).findFirst()
				.map(ProcessorDTO::getId).map(flowController::getProcessorNode).map(this::newProcessor);
	}

	private Optional<TestRunner> getProcessorByName(String processorName) {
		if (logger.isDebugEnabled()) {
			logger.debug("Get processor for name " + processorName);
		}
		return template.getSnippet().getProcessors().stream()
				.filter(processorDTO -> processorDTO.getName().equals(processorName)).findFirst()
				.map(ProcessorDTO::getId).map(flowController::getProcessorNode).map(this::newProcessor);
	}

	private TestRunner newProcessor(ProcessorNode processorNode) {
		Processor processor = processorNode.getProcessor();
		TestRunner newProcessor = TestRunners.newTestRunner(processor);
		if (processor instanceof ConsumeKafka_0_10 || processor instanceof ConsumeKafkaRecord_0_10) {
			// https://github.com/apache/nifi/commit/4bfb905f37d5084f70f228185431c16ce73369fb#r31636428
			newProcessor.setValidateExpressionUsage(false);
		}
		updateProcessorProperties(processorNode, newProcessor);
		enableProcessorControllersServices(processorNode, newProcessor);
		return newProcessor;
	}

	private void updateProcessorProperties(ProcessorNode processorNode, TestRunner newProcessor) {
		processorNode.getProperties().entrySet().stream()
		.filter(entry -> Objects.nonNull(entry.getValue())).forEach(entry -> {
			newProcessor.setProperty(entry.getKey(), entry.getValue());
		});
	}

	private void enableProcessorControllersServices(ProcessorNode processorNode, TestRunner testRunner) {
		testRunner.getProcessor().getPropertyDescriptors().stream()
				.filter(propertyDesc -> propertyDesc.getControllerServiceDefinition() != null)
				.map(processorNode::getProperty).filter(Objects::nonNull).map(flowController::getControllerServiceNode)
				.forEach(controllerServiceNode -> enableControllerService(controllerServiceNode, testRunner));
	}

	private void enableControllerService(ControllerServiceNode controllerServiceNode, TestRunner testRunner) {
		ControllerService controllerService = controllerServiceNode.getControllerServiceImplementation();
		if (logger.isDebugEnabled()) {
			logger.debug("Enable controler service [" + controllerService.getClass().getName() + "]");
		}
		try {
			Map<String, String> properties = buildProperties(controllerServiceNode, testRunner);
			testRunner.addControllerService(controllerService.getIdentifier(), controllerService, properties);
			testRunner.enableControllerService(controllerService);
		} catch (InitializationException e) {
			logger.error(e.getMessage(), e);
		}
	}

	private Map<String, String> buildProperties(ControllerServiceNode controllerServiceNode, TestRunner testRunner) {
		ControllerService controllerService = controllerServiceNode.getControllerServiceImplementation();

		enableParentControllersServices(controllerServiceNode, testRunner, controllerService);

		return controllerService.getPropertyDescriptors().stream()
				.map(p -> new Tuple<>(p.getName(), controllerServiceNode.getProperty(p)))
				.filter(tuple -> tuple.getValue() != null).collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));
	}

	private void enableParentControllersServices(ControllerServiceNode controllerServiceNode, TestRunner testRunner,
			ControllerService controllerService) {
		controllerService.getPropertyDescriptors().stream()
				.filter(propertyDesc -> propertyDesc.getControllerServiceDefinition() != null)
				.map(propertyDesc -> controllerServiceNode.getProperty(propertyDesc)).filter(Objects::nonNull)
				.map(flowController::getControllerServiceNode).forEach(csn -> enableControllerService(csn, testRunner));
	}
}
