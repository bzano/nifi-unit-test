# NiFi Unit Test Framework

## USAGE
- maven dependency

``` maven
<dependency>
	<groupId>org.bzano.nifi</groupId>
	<artifactId>nifi-unit-tests</artifactId>
	<version>0.0.1-SNAPSHOT</version>
</dependency>
```

- Unit Test Class

``` java
public class LogMergeRecordTest {
	private static final Logger logger = LoggerFactory.getLogger(LogMergeRecordTest.class);

	private static final String TEMPLATE_NAME = "template.xml";
	
	@NiFiEntity(name = "Processor")
	TestRunner processor;

	@Before
	public void setup() throws NiFiMockInitException {
		String templateFilePath = this.getClass().getClassLoader().getResource(TEMPLATE_NAME).getPath();
		NiFiMock.init(new File(templateFilePath), this);
		logger.info("template initialized");
	}
	
	@Test
	public void logsQueryRecord_should_return_merged_event() {
		// GIVEN input message
		processor.enqueue(EVENT);
		// WHEN run the processor on a single thread
		processor.run(1);
		// THEN get the first flow file routed to the success queue
		processor.getFlowFilesForRelationship("success").get(0).assertContentEquals(EVENT);
	}
}
```
- Template

``` java
	private static final String TEMPLATE_NAME = "template.xml";
```
This is the template name you can download from NiFi after you finished designing your flow

The template should be in the resources folder

- Processor

```java
	@NiFiEntity(name = "Processor")
	TestRunner processor;
```

UT is about testing each processor separately from the others

We can get processors we need with the NiFiEntity annotation