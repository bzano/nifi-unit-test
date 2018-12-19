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
public class ProcessorTest {
	private static final Logger logger = LoggerFactory.getLogger(LogMergeRecordTest.class);

	private static final String TEMPLATE_NAME = "path_to_template.xml";
	
	@NiFiEntity(name = "Processor")
	TestRunner processor;

	@Before
	public void setup() throws NiFiMockInitException {
		NiFiMock.init(new File(TEMPLATE_NAME), this);
		logger.info("template initialized");
	}
	
	@Test
	public void processor_should_return_same_event() {
		// GIVEN input message
		processor.enqueue(EVENT);
		// WHEN run the processor on a single thread
		processor.run();
		// THEN get the first flow file routed to the success queue
		processor.getFlowFilesForRelationship("success").get(0).assertContentEquals(EVENT);
	}
}
```
- Template

``` java
	private static final String TEMPLATE_NAME = "path_to_template.xml";
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

- Run processors

To send events as input to the processor

```java
	processor.enqueue(EVENT);
```
To Run the processor 

```java
	processor.run();
```
And then get the generated flow files

```java
	processor.getFlowFilesForRelationship("success").get(0).assertContentEquals(EVENT);
```
