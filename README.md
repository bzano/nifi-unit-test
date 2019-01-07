# [NiFi Unit Test Framework](https://github.com/bzano/nifi-unit-test/)
The Nifi Unit Test Framework allows you easily to perform unit tests on your Nifi components from a given Nifi template.
The Framework is developed in Java 1.8.

Table of contents
=================

<!-- MarkdownTOC -->
* [UNSUPORTED YET](#unsuported)
* [Usage](#usage)
  * [Maven dependency](#maven-dependency)
  * [Configuration](#configuration)
    * [Template](#template)
    * [Processor](#processor)
    * [Run a processor](#run-a-processor)
  * [Example](#example)
<!-- /MarkdownTOC -->

<a name="unsuported">UNSUPORTED YET</a>
------------

- HDFS Components

<a name="usage">Usage</a>
------------

### Maven dependency

``` maven
<dependency>
	<groupId>org.bzano.nifi</groupId>
	<artifactId>nifi-unit-tests</artifactId>
	<version>0.0.1-SNAPSHOT</version>
</dependency>
```

### Configuration
#### Template
Specify the template name you can download from NiFi after you finished designing your flow.

The template should be in the resources folder.

``` java
private static final String TEMPLATE_NAME = "path_to_template.xml"; // Specify the root filesystem path
```

### Processor
UT is about testing each processor separately from the others

We can get processors we need with the NiFiEntity annotation

```java
@NiFiEntity(name = "Processor") // Your processor name
TestRunner processor;
```



### Run a processor

To send events as input to the processor

```java
processor.enqueue(EVENT); // The input String or bytes[] message depending on the processor
```
To Run the processor 

```java
processor.run();
```
And then get the generated flow files

```java
processor.getFlowFilesForRelationship("success").get(0).assertContentEquals(EVENT);
```


## Example

``` java
public class ProcessorTest {
	private static final String TEMPLATE_NAME = "path_to_template.xml";
	
	@NiFiEntity(name = "Processor")
	TestRunner processor; // Will be init in the setUp() method, via NifiMock.init function

	@Before
	public void setup() throws NiFiMockInitException {
		NiFiMock.init(new File(TEMPLATE_NAME), this);
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
