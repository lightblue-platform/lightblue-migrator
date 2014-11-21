package com.redhat.lightblue.migrator.consistency;


import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobConfigurationTest {

	private static final String hostName = "lightblue.io";
	private static final int threadCount = 63;
	
	private static final List<String> lightblueEntityKeyFields = Arrays.asList("id","name");;
	private static final String lightblueEntityTimestampField = "lastUpdateDate";
	private static final List<String> legacyEntityKeyFields = Arrays.asList("id","name");;;
	private static final String legacyEntityTimestampField = "lastUpdateDate";
	
	private static final String legacyServiceURI = "http://demo.lightblue.io/rest/data/";
	private static final String lightblueServiceURI = "http://demo.lightblue.io/rest/data/";
	private static final String lightblueEntityVersion = "lightblueEntityVersion";
	private static final String lightblueEntityName = "lightblueEntityName";
	private static final String legacyEntityVersion = "legacyEntityVersion";
	private static final String legacyEntityName = "legacyEntityName";
	
	JobConfiguration configuration;
	
	@Before
	public void setUp() throws Exception {
		configuration = new JobConfiguration();
		configuration.setHostName(hostName);
		configuration.setThreadCount(threadCount);
		configuration.setOverwriteLightblueDocuments(true);
		configuration.setLightblueEntityKeyFields(lightblueEntityKeyFields);
		configuration.setLightblueEntityTimestampField(lightblueEntityTimestampField);
		configuration.setLegacyEntityKeyFields(legacyEntityKeyFields);
		configuration.setLegacyEntityTimestampField(legacyEntityTimestampField);
		configuration.setLegacyEntityName(legacyEntityName);
		configuration.setLegacyEntityVersion(legacyEntityVersion);
		configuration.setLightblueEntityName(lightblueEntityName);
		configuration.setLightblueEntityVersion(lightblueEntityVersion);
		configuration.setLegacyServiceURI(legacyServiceURI);
		configuration.setLightblueServiceURI(lightblueServiceURI);
		configuration.setOverwriteLightblueDocuments(true);
	}
	
	@Test
	public void testGetHostname() {
		Assert.assertEquals(hostName, configuration.getHostName());
	}

	@Test
	public void testSetHostName() {
		configuration.setHostName(legacyEntityName);
		Assert.assertEquals(legacyEntityName, configuration.getHostName());
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
	public void testGetLightblueEntityName() {
		Assert.assertEquals(lightblueEntityName, configuration.getLightblueEntityName());
	}

	@Test
	public void testSetLightblueEntityName() {
		configuration.setLightblueEntityName(legacyEntityName);
		Assert.assertEquals(legacyEntityName, configuration.getLightblueEntityName());
	}

	@Test
	public void testGetLightblueEntityVersion() {
		Assert.assertEquals(lightblueEntityVersion, configuration.getLightblueEntityVersion());
	}

	@Test
	public void testSetLightblueEntityVersion() {
		configuration.setLightblueEntityVersion(lightblueEntityName);
		Assert.assertEquals(lightblueEntityName, configuration.getLightblueEntityVersion());
	}

	@Test
	public void testGetLightblueEntityKeyFields() {
		Assert.assertEquals(lightblueEntityKeyFields, configuration.getLightblueEntityKeyFields());
	}

	@Test
	public void testSetLightblueEntityKeyFields() {
		configuration.setLightblueEntityKeyFields(legacyEntityKeyFields);
		Assert.assertEquals(legacyEntityKeyFields, configuration.getLightblueEntityKeyFields());
	}
	
	@Test
	public void testGetLightblueEntityTimestampField() {
		Assert.assertEquals(lightblueEntityTimestampField, configuration.getLightblueEntityTimestampField());
	}

	@Test
	public void testSetLightblueEntityTimestampField() {
		configuration.setLightblueEntityTimestampField(legacyEntityTimestampField);
		Assert.assertEquals(legacyEntityTimestampField, configuration.getLightblueEntityTimestampField());
	}
	
	@Test
	public void testGetLegacyEntityName() {
		Assert.assertEquals(legacyEntityName, configuration.getLegacyEntityName());
	}

	@Test
	public void testSetLegacyEntityName() {
		configuration.setLegacyEntityName(lightblueEntityName);
		Assert.assertEquals(lightblueEntityName, configuration.getLegacyEntityName());
	}

	@Test
	public void testGetLegacyEntityVersion() {
		Assert.assertEquals(legacyEntityVersion, configuration.getLegacyEntityVersion());
	}

	@Test
	public void testSetLegacyEntityVersion() {
		configuration.setLegacyEntityVersion(lightblueEntityVersion);
		Assert.assertEquals(lightblueEntityVersion, configuration.getLegacyEntityVersion());
	}

	@Test
	public void testGetLegacyEntityKeyFields() {
		Assert.assertEquals(legacyEntityKeyFields, configuration.getLegacyEntityKeyFields());
	}

	@Test
	public void testSetLegacyEntityKeyFields() {
		configuration.setLegacyEntityKeyFields(legacyEntityKeyFields);
		Assert.assertEquals(legacyEntityKeyFields, configuration.getLegacyEntityKeyFields());
	}
	
	@Test
	public void testGetLegacyEntityTimestampField() {
		Assert.assertEquals(legacyEntityTimestampField, configuration.getLegacyEntityTimestampField());
	}

	@Test
	public void testSetLegacyEntityTimestampField() {
		configuration.setLegacyEntityTimestampField(legacyEntityTimestampField);
		Assert.assertEquals(legacyEntityTimestampField, configuration.getLegacyEntityTimestampField());
	}
	
	@Test
	public void testGetLightblueServiceURI() {
		Assert.assertEquals(lightblueServiceURI, configuration.getLightblueServiceURI());
	}

	@Test
	public void testSetLightblueServiceURI() {
		configuration.setLightblueEntityName(legacyServiceURI);
		Assert.assertEquals(legacyServiceURI, configuration.getLightblueServiceURI());
	}

	@Test
	public void testGetLegacyServiceURI() {
		Assert.assertEquals(legacyServiceURI, configuration.getLegacyServiceURI());
	}

	@Test
	public void testSetLegacyServiceURI() {
		configuration.setLegacyEntityName(lightblueServiceURI);
		Assert.assertEquals(lightblueServiceURI, configuration.getLegacyServiceURI());
	}

	@Test
	public void testSetOverwriteLightblueDocuments() {
		configuration.setOverwriteLightblueDocuments(false);
		Assert.assertEquals(false, configuration.shouldOverwriteLightblueDocuments());
	}

	@Test
	public void testShouldOverwriteLightblueDocuments() {
		Assert.assertEquals(true, configuration.shouldOverwriteLightblueDocuments());
	}

}
