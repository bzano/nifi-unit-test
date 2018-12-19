package org.ut.nifi.mock;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.TemplateUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class NiFiTemplate {
	private static final Logger logger = LoggerFactory.getLogger(NiFiTemplate.class);

	private static final String TEMPLATE_NOT_INITIALIZED = "Template not initialized";
	private static final String BUNDLE_GROUP_ID = "org.soge.nifi";
	private static final String BUNDLE_ARTIFACT_ID = "nifi-unit-tests";
	private static final String BUNDLE_VERSION = "0.0.1-SNAPSHOT";
	
	private File xmlFile;
	private FlowController flowController;
	private TemplateDTO template;
	
	public NiFiTemplate(File xmlFile) {
		this.xmlFile = xmlFile;
	}
	
	public TemplateDTO init() throws NiFiMockInitException {
		assertFileExists();
		loadTemplate();
		updateBundles();
		loadFlowController();
		return template;
	}

	private void loadFlowController() throws NiFiMockInitException {
		flowController = new NiFiFlowController(template).init();
	}

	private void loadTemplate() throws NiFiMockInitException {
		logger.info("Loading template from " + xmlFile.getPath());
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(TemplateDTO.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			template = (TemplateDTO) jaxbUnmarshaller.unmarshal(xmlFile);
			TemplateUtils.scrubTemplate(template);
		}catch(JAXBException e) {
			throw new NiFiMockInitException(e);
		}
	}

	private void updateBundles() {
		FlowSnippetDTO snippet = template.getSnippet();

		updateProcessorsBundle(snippet);
		updateControllerServicesBundle(snippet);
	}
	
	public TemplateDTO getTemplate() throws NiFiMockInitException {
		if(template == null) {
			throw new NiFiMockInitException(new InitializationError(TEMPLATE_NOT_INITIALIZED));
		}
		return template;
	}
	
	public FlowController getFlowController() throws NiFiMockInitException {
		if(flowController == null) {
			throw new NiFiMockInitException(new InitializationError(TEMPLATE_NOT_INITIALIZED));
		}
		return flowController;
	}

	private void assertFileExists() throws NiFiMockInitException {
		if (!xmlFile.exists()) {
			logger.debug(xmlFile.getPath() + " not found");
			throw new NiFiMockInitException(new FileNotFoundException(xmlFile.getPath()));
		}
	}

	private void updateControllerServicesBundle(FlowSnippetDTO snippet) {
		logger.info("Update controllers services bundle to " + BUNDLE_GROUP_ID + ":" + BUNDLE_ARTIFACT_ID + ":" + BUNDLE_VERSION);
		snippet.getControllerServices().forEach(controllerService -> {
			BundleDTO bundle = controllerService.getBundle();
			bundle.setGroup(BUNDLE_GROUP_ID);
			bundle.setArtifact(BUNDLE_ARTIFACT_ID);
			bundle.setVersion(BUNDLE_VERSION);
		});
	}

	private void updateProcessorsBundle(FlowSnippetDTO snippet) {
		logger.info("Update processors bundle to " + BUNDLE_GROUP_ID + ":" + BUNDLE_ARTIFACT_ID + ":" + BUNDLE_VERSION);
		snippet.getProcessors().forEach(processor -> {
			BundleDTO bundle = processor.getBundle();
			bundle.setGroup(BUNDLE_GROUP_ID);
			bundle.setArtifact(BUNDLE_ARTIFACT_ID);
			bundle.setVersion(BUNDLE_VERSION);
		});
	}
}
