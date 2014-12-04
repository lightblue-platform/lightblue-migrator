package com.redhat.lightblue.migrator.consistency;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MigrationConfigurationTest {

	private static final String hostName = "lightblue.io";
	private static final int threadCount = 63;

	private static final List<String> destinationEntityKeyFields = Arrays.asList("id", "name");;
	private static final String destinationEntityTimestampField = "lastUpdateDate";
	private static final List<String> sourceEntityKeyFields = Arrays.asList("id", "name");;;
	private static final String sourceEntityTimestampField = "lastUpdateDate";

	private static final String destinationEntityVersion = "destinationEntityVersion";
	private static final String destinationEntityName = "destinationEntityName";
	private static final String sourceEntityVersion = "sourceEntityVersion";
	private static final String sourceEntityName = "sourceEntityName";

	MigrationConfiguration configuration;

	@Before
	public void setUp() throws Exception {
		configuration = new MigrationConfiguration();
		configuration.setHostName(hostName);
		configuration.setThreadCount(threadCount);
		configuration.setOverwriteDestinationDocuments(true);
		configuration.setDestinationEntityKeyFields(destinationEntityKeyFields);
		configuration.setDestinationEntityTimestampField(destinationEntityTimestampField);
		configuration.setSourceEntityKeyFields(sourceEntityKeyFields);
		configuration.setSourceEntityTimestampField(sourceEntityTimestampField);
		configuration.setSourceEntityName(sourceEntityName);
		configuration.setSourceEntityVersion(sourceEntityVersion);
		configuration.setDestinationEntityName(destinationEntityName);
		configuration.setDestinationEntityVersion(destinationEntityVersion);
		configuration.setOverwriteDestinationDocuments(true);
	}

	@Test
	public void testGetHostname() {
		Assert.assertEquals(hostName, configuration.getHostName());
	}

	@Test
	public void testSetHostName() {
		configuration.setHostName(sourceEntityName);
		Assert.assertEquals(sourceEntityName, configuration.getHostName());
	}

	@Test
	public void testThreadCount() {
		Assert.assertEquals(threadCount, configuration.getThreadCount());
	}

	@Test
	public void testSetThreadCount() {
		configuration.setThreadCount(2);
		Assert.assertEquals(2, configuration.getThreadCount());
	}

	@Test
	public void testGetDestinationEntityName() {
		Assert.assertEquals(destinationEntityName, configuration.getDestinationEntityName());
	}

	@Test
	public void testSetDestinationEntityName() {
		configuration.setDestinationEntityName(sourceEntityName);
		Assert.assertEquals(sourceEntityName, configuration.getDestinationEntityName());
	}

	@Test
	public void testGetDestinationEntityVersion() {
		Assert.assertEquals(destinationEntityVersion, configuration.getDestinationEntityVersion());
	}

	@Test
	public void testSetDestinationEntityVersion() {
		configuration.setDestinationEntityVersion(destinationEntityName);
		Assert.assertEquals(destinationEntityName, configuration.getDestinationEntityVersion());
	}

	@Test
	public void testGetDestinationEntityKeyFields() {
		Assert.assertEquals(destinationEntityKeyFields, configuration.getDestinationEntityKeyFields());
	}

	@Test
	public void testSetDestinationEntityKeyFields() {
		configuration.setDestinationEntityKeyFields(sourceEntityKeyFields);
		Assert.assertEquals(sourceEntityKeyFields, configuration.getDestinationEntityKeyFields());
	}

	@Test
	public void testGetDestinationEntityTimestampField() {
		Assert.assertEquals(destinationEntityTimestampField, configuration.getDestinationEntityTimestampField());
	}

	@Test
	public void testSetDestinationEntityTimestampField() {
		configuration.setDestinationEntityTimestampField(sourceEntityTimestampField);
		Assert.assertEquals(sourceEntityTimestampField, configuration.getDestinationEntityTimestampField());
	}

	@Test
	public void testGetSourceEntityName() {
		Assert.assertEquals(sourceEntityName, configuration.getSourceEntityName());
	}

	@Test
	public void testSetSourceEntityName() {
		configuration.setSourceEntityName(destinationEntityName);
		Assert.assertEquals(destinationEntityName, configuration.getSourceEntityName());
	}

	@Test
	public void testGetSourceEntityVersion() {
		Assert.assertEquals(sourceEntityVersion, configuration.getSourceEntityVersion());
	}

	@Test
	public void testSetSourceEntityVersion() {
		configuration.setSourceEntityVersion(destinationEntityVersion);
		Assert.assertEquals(destinationEntityVersion, configuration.getSourceEntityVersion());
	}

	@Test
	public void testGetSourceEntityKeyFields() {
		Assert.assertEquals(sourceEntityKeyFields, configuration.getSourceEntityKeyFields());
	}

	@Test
	public void testSetSourceEntityKeyFields() {
		configuration.setSourceEntityKeyFields(sourceEntityKeyFields);
		Assert.assertEquals(sourceEntityKeyFields, configuration.getSourceEntityKeyFields());
	}

	@Test
	public void testGetSourceEntityTimestampField() {
		Assert.assertEquals(sourceEntityTimestampField, configuration.getSourceEntityTimestampField());
	}

	@Test
	public void testSetSourceEntityTimestampField() {
		configuration.setSourceEntityTimestampField(sourceEntityTimestampField);
		Assert.assertEquals(sourceEntityTimestampField, configuration.getSourceEntityTimestampField());
	}

	@Test
	public void testSetOverwriteDestinationDocuments() {
		configuration.setOverwriteDestinationDocuments(false);
		Assert.assertEquals(false, configuration.shouldOverwriteDestinationDocuments());
	}

	@Test
	public void testShouldOverwriteDestinationDocuments() {
		Assert.assertEquals(true, configuration.shouldOverwriteDestinationDocuments());
	}

}
