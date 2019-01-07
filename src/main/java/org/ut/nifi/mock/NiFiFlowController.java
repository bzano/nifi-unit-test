package org.ut.nifi.mock;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.impl.StandardAuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.StandardManagedAuthorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.service.ControllerServiceLoader;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.StandardFlowRegistryClient;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.MockVariableRegistry;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ut.nifi.TestFlowFileRepository;
import org.ut.nifi.mock.exceptions.NiFiMockInitException;

public class NiFiFlowController {
	private static final Logger logger = LoggerFactory.getLogger(NiFiFlowController.class);

	private static final String BUNDLE_GROUP_ID = "org.soge.nifi";
	private static final String BUNDLE_ARTIFACT_ID = "nifi-unit-tests";
	private static final String BUNDLE_VERSION = "0.0.1-SNAPSHOT";

	private static final String ENCRYPTOR_ALGO = "PBEWITHSHAANDTWOFISH-CBC";
	private static final String ENCRYPTOR_PROVIDER = "BC";
	private static final String ENCRYPTOR_KEY = "NO_KEY";

	private static final String FLOWFILE_REPOSITORY_DIRECTORY_VALUE = "target/nifi/flow-repo";
	private static final String REPOSITORY_CONTENT = NiFiProperties.REPOSITORY_CONTENT_PREFIX + "repo.1";
	private static final String REPOSITORY_CONTENT_VALUE = "target/nifi/flow-repo/repos";
	private static final String STATE_MANAGEMENT_PATH = "conf/state-management.xml";
	private static final String STATE_MANAGEMENT_LOCAL_PROVIDER_ID_value = "local-provider";

	private TemplateDTO template;

	protected NiFiFlowController(TemplateDTO template) {
		this.template = template;
	}

	protected FlowController init() throws NiFiMockInitException {
		loadBundles(getClass().getClassLoader());
		FlowController flowController  = initFlowController();

		instantiateSnippet(flowController);

		initFlow(flowController);

		return flowController;
	}

	private void instantiateSnippet(FlowController flowController) throws NiFiMockInitException {
		try {
			Optional<ProcessGroup> processGroup = template.getSnippet().getProcessors().stream()
					.map(ProcessorDTO::getParentGroupId).findFirst().map(flowController::createProcessGroup);
			processGroup.ifPresent(flowController::onProcessGroupAdded);
			if (processGroup.isPresent()) {
				flowController.instantiateSnippet(processGroup.get(), template.getSnippet());
			}
		} catch (ProcessorInstantiationException e) {
			throw new NiFiMockInitException(e);
		}
	}

	private void initFlow(FlowController flowController) throws NiFiMockInitException {
		logger.info("Init flows by enabling controllers services & validating processors");
		try {
			flowController.initializeFlow();
			Set<ControllerServiceNode> allControllerServices = flowController.getAllControllerServices();
			ControllerServiceLoader.enableControllerServices(allControllerServices, flowController, true);
			performValidateProcessors(flowController, template);
		} catch (IOException e) {
			throw new NiFiMockInitException(e);
		}
	}

	private void loadBundles(ClassLoader classLoader) {
		logger.info("Add NiFi extensions " + BUNDLE_GROUP_ID + ":" + BUNDLE_ARTIFACT_ID + ":" + BUNDLE_VERSION);
		BundleCoordinate bundleCoordinate = new BundleCoordinate(BUNDLE_GROUP_ID, BUNDLE_ARTIFACT_ID, BUNDLE_VERSION);

		BundleDetails bundleDetails = new BundleDetails.Builder().coordinate(bundleCoordinate).workingDir(new File("."))
				.build();

		Bundle systemBundle = new Bundle(bundleDetails, classLoader);

		ExtensionManager.discoverExtensions(systemBundle, new HashSet<>());
	}

	private void performValidateProcessors(FlowController flowController, TemplateDTO template) {
		logger.info("Validate processors");
		template.getSnippet().getProcessors().stream().map(pr -> flowController.getProcessorNode(pr.getId()))
				.forEach(pn -> pn.performValidation());
	}

	private FlowController initFlowController() {
		logger.info("Init flowController by default values");
		FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(10);
		NiFiProperties nifiProperties = getNIFIProperties();
		Authorizer authorizer = new StandardManagedAuthorizer();
		AuditService auditService = new StandardAuditService();
		StringEncryptor stringEncryptor = StringEncryptor.createEncryptor(ENCRYPTOR_ALGO, ENCRYPTOR_PROVIDER,
				ENCRYPTOR_KEY);
		BulletinRepository bulletinRepository = new VolatileBulletinRepository();
		VariableRegistry variableRegistry = new MockVariableRegistry();
		FlowRegistryClient flowRegistryClient = new StandardFlowRegistryClient();

		FlowController flowController = FlowController.createStandaloneInstance(flowFileEventRepository, nifiProperties,
				authorizer, auditService, stringEncryptor, bulletinRepository, variableRegistry, flowRegistryClient);
		return flowController;
	}

	private StandardNiFiProperties getNIFIProperties() {
		Properties props = new Properties();

		props.put(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, FLOWFILE_REPOSITORY_DIRECTORY_VALUE);
		props.put(REPOSITORY_CONTENT, REPOSITORY_CONTENT_VALUE);
		props.put(NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID, STATE_MANAGEMENT_LOCAL_PROVIDER_ID_value);
		props.put(NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION, TestFlowFileRepository.class.getCanonicalName());
		props.put(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, getStateManagementPath());

		StandardNiFiProperties properties = new StandardNiFiProperties(props);
		return properties;
	}

	private String getStateManagementPath() {
		ClassLoader classLoader = NiFiMock.class.getClassLoader();
		URL stateManageement = classLoader.getResource(STATE_MANAGEMENT_PATH);
		String stateManagementFilePath = stateManageement.getPath();
		return stateManagementFilePath;
	}
}
