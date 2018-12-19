package org.ut.nifi.mock;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class NiFiMock {
	private static final Logger logger = LoggerFactory.getLogger(NiFiMock.class);

	private static final Map<String, NiFiTemplate> TEMPLATES = new HashMap<>();

	public static void init(File xmlFile, Object caller) throws NiFiMockInitException {
		NiFiMockProperty nifiMockProperty = initNifiMockProperty(xmlFile, caller);
		nifiMockProperty.updateFieldsProcessor();
	}

	private static NiFiMockProperty initNifiMockProperty(File xmlFile, Object caller) throws NiFiMockInitException {
		logger.info("Init NiFiMockProperty with " + xmlFile.getPath());
		String xmlFilePath = xmlFile.getPath();
		NiFiTemplate templateLoader = TEMPLATES.get(xmlFilePath);
		if(templateLoader == null) {
			logger.info("No TemplateLoader found a new one will be created for " + xmlFile.getPath());
			templateLoader = initTemplate(xmlFile);
			TEMPLATES.put(xmlFilePath, templateLoader);
		}
		return newNifiMockProperty(templateLoader, caller);
	}

	private static NiFiMockProperty newNifiMockProperty(NiFiTemplate templateLoader, Object caller) throws NiFiMockInitException {
		logger.info("Create new NiFiMockProperty for " + caller.getClass().getName());
		NiFiMockProperty nifiMockProperty;
		TemplateDTO template = templateLoader.getTemplate();
		FlowController flowController = templateLoader.getFlowController();
		nifiMockProperty = new NiFiMockProperty(flowController, template, caller);
		return nifiMockProperty;
	}

	private static NiFiTemplate initTemplate(File xmlFile) throws NiFiMockInitException {
		NiFiTemplate templateLoader = new NiFiTemplate(xmlFile);
		templateLoader.init();
		return templateLoader;
	}
}
