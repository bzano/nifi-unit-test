package org.ut.nifi;

import java.io.IOException;
import java.util.Collection;

import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.WriteAheadFlowFileRepository;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFlowFileRepository extends WriteAheadFlowFileRepository {
	
	private static final Logger logger = LoggerFactory.getLogger(TestFlowFileRepository.class);
	
	public TestFlowFileRepository() {
		super();
	}
	
	public TestFlowFileRepository(NiFiProperties properties) {
		super(properties);
	}
	
	@Override
	public void updateRepository(Collection<RepositoryRecord> records) throws IOException {
		super.updateRepository(records);
		logger.info(records.toString());
	}
}
