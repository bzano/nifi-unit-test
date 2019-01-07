package org.ut.nifi.mock;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.processors.hadoop.AbstractHadoopProcessor;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class NiFiMock {
	private static final Logger logger = LoggerFactory.getLogger(NiFiMock.class);

	private static final Map<String, NiFiTemplate> TEMPLATES = new HashMap<>();

	public static void init(File xmlFile, Object caller) throws NiFiMockInitException {
		updateKerberosCredetialService();
		NiFiMockProperty nifiMockProperty = initNifiMockProperty(xmlFile, caller);
		nifiMockProperty.updateFieldsProcessor();
	}
	
	
	private static void updateKerberosCredetialService() {
		try {
			Field KerberosCredServiceField = AbstractHadoopProcessor.class.getDeclaredField("KERBEROS_CREDENTIALS_SERVICE");
			Field controllerServiceDefinitionField = PropertyDescriptor.class.getDeclaredField("controllerServiceDefinition");
			KerberosCredServiceField.setAccessible(true);
			controllerServiceDefinitionField.setAccessible(true);
			
			Object value = KerberosCredServiceField.get(null);
			controllerServiceDefinitionField.set(value, null);
			
			KerberosCredServiceField.setAccessible(false);
			controllerServiceDefinitionField.setAccessible(false);
			logger.info(value.toString());
		} catch (NoSuchFieldException e) {
			logger.error(e.getMessage(), e);
		} catch (SecurityException e) {
			logger.error(e.getMessage(), e);
		} catch (IllegalArgumentException e) {
			logger.error(e.getMessage(), e);
		} catch (IllegalAccessException e) {
			logger.error(e.getMessage(), e);
		}
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
