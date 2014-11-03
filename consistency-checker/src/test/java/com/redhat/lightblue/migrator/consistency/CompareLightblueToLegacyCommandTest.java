package com.redhat.lightblue.migrator.consistency;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;

public class CompareLightblueToLegacyCommandTest {

	private static final int documentsUpdated = 42;
	private static final int inconsistentDocuments = 42;
	private static final int documentsCompared = 63;
	private static final String legacyServiceURI = "http://demo.lightblue.io/rest/data/";
	private static final String lightblueServiceURI = "http://demo.lightblue.io/rest/data/";
	private static final String lightblueEntityVersion = "lightblueEntityVersion";
	private static final String lightblueEntityName = "lightblueEntityName";
	private static final String legacyEntityVersion = "legacyEntityVersion";
	private static final String legacyEntityName = "legacyEntityName";
	private static final String legacyFindJsonExpression = "{}";
	private static final String lightblueFindJsonExpression = "{}";
	private static final String lightblueUpdateJsonExpression = "{}";

	CompareLightblueToLegacyCommand command;
	LightblueClient client;
	
	@Before
	public void setUp() throws Exception {
		command = new CompareLightblueToLegacyCommand();
		command.setDocumentsCompared(documentsCompared);
		command.setInconsistentDocuments(inconsistentDocuments);
		command.setLightblueDocumentsUpdated(documentsUpdated);
		command.setOverwriteLightblueDocuments(true);
		command.setLegacyEntityName(legacyEntityName);
		command.setLegacyEntityVersion(legacyEntityVersion);
		command.setLightblueEntityName(lightblueEntityName);
		command.setLightblueEntityVersion(lightblueEntityVersion);
		command.setLegacyServiceURI(legacyServiceURI);
		command.setLightblueServiceURI(lightblueServiceURI);
		command.setLegacyFindJsonExpression(legacyFindJsonExpression);
		command.setLightblueFindJsonExpression(lightblueFindJsonExpression);
		command.setLightblueUpdateJsonExpression(lightblueUpdateJsonExpression);
		command.setOverwriteLightblueDocuments(true);
		command.setHasFailures(true);
		client = new LightblueHttpClient();
		command.setClient(client);
	}

	@Test
	public void testGetDocumentsCompared() {
		Assert.assertEquals(documentsCompared, command.getDocumentsCompared());
	}

	@Test
	public void testSetDocumentsCompared() {
		command.setDocumentsCompared(documentsUpdated);
		Assert.assertEquals(documentsUpdated, command.getDocumentsCompared());
	}

	@Test
	public void testGetInconsistentDocuments() {
		Assert.assertEquals(inconsistentDocuments, command.getInconsistentDocuments());
	}

	@Test
	public void testSetInconsistentDocuments() {
		command.setInconsistentDocuments(documentsUpdated);
		Assert.assertEquals(documentsUpdated, command.getInconsistentDocuments());
	}

	@Test
	public void testGetLightblueDocumentsUpdated() {
		Assert.assertEquals(documentsUpdated, command.getLightblueDocumentsUpdated());
	}

	@Test
	public void testSetLightblueDocumentsUpdated() {
		command.setLightblueDocumentsUpdated(documentsCompared);
		Assert.assertEquals(documentsCompared, command.getLightblueDocumentsUpdated());
	}

	@Test
	public void testGetLightblueEntityName() {
		Assert.assertEquals(lightblueEntityName, command.getLightblueEntityName());
	}

	@Test
	public void testSetLightblueEntityName() {
		command.setLightblueEntityName(legacyEntityName);
		Assert.assertEquals(legacyEntityName, command.getLightblueEntityName());
	}

	@Test
	public void testGetLightblueEntityVersion() {
		Assert.assertEquals(lightblueEntityVersion, command.getLightblueEntityVersion());
	}

	@Test
	public void testSetLightblueEntityVersion() {
		command.setLightblueEntityVersion(lightblueEntityName);
		Assert.assertEquals(lightblueEntityName, command.getLightblueEntityVersion());
	}

	@Test
	public void testGetLegacyEntityName() {
		Assert.assertEquals(legacyEntityName, command.getLegacyEntityName());
	}

	@Test
	public void testSetLegacyEntityName() {
		command.setLegacyEntityName(lightblueEntityName);
		Assert.assertEquals(lightblueEntityName, command.getLegacyEntityName());
	}

	@Test
	public void testGetLegacyEntityVersion() {
		Assert.assertEquals(legacyEntityVersion, command.getLegacyEntityVersion());
	}

	@Test
	public void testSetLegacyEntityVersion() {
		command.setLegacyEntityVersion(lightblueEntityVersion);
		Assert.assertEquals(lightblueEntityVersion, command.getLegacyEntityVersion());
	}

	@Test
	public void testGetLegacyFindJsonExpression() {
		Assert.assertEquals(legacyFindJsonExpression, command.getLegacyFindJsonExpression());
	}

	@Test
	public void testSetLegacyFindJsonExpression() {
		command.setLegacyFindJsonExpression(lightblueFindJsonExpression);
		Assert.assertEquals(lightblueFindJsonExpression, command.getLegacyFindJsonExpression());
	}

	@Test
	public void testGetLightblueFindJsonExpression() {
		Assert.assertEquals(lightblueFindJsonExpression, command.getLightblueFindJsonExpression());
	}

	@Test
	public void testSetLightblueFindJsonExpression() {
		command.setLightblueFindJsonExpression(lightblueFindJsonExpression);
		Assert.assertEquals(lightblueFindJsonExpression, command.getLegacyFindJsonExpression());
	}

	@Test
	public void testGetLightblueUpdateJsonExpression() {
		Assert.assertEquals(lightblueUpdateJsonExpression, command.getLightblueUpdateJsonExpression());
	}

	@Test
	public void testSetLightblueUpdateJsonExpression() {
		command.setLightblueUpdateJsonExpression(legacyFindJsonExpression);
		Assert.assertEquals(legacyFindJsonExpression, command.getLightblueUpdateJsonExpression());
	}

	@Test
	public void testHasFailures() {
		Assert.assertTrue(command.hasFailures());
	}

	@Test
	public void testSetHasFailures() {
		command.setHasFailures(false);
		Assert.assertFalse(command.hasFailures());
	}

	@Test
	public void testGetClient() {
		Assert.assertEquals(client, command.getClient());
	}

	@Test
	public void testSetClient() {
		LightblueClient newClient = new LightblueHttpClient();
		command.setClient(newClient);
		Assert.assertEquals(newClient, command.getClient());
	}

	@Test
	public void testGetLightblueServiceURI() {
		Assert.assertEquals(lightblueServiceURI, command.getLightblueServiceURI());
	}

	@Test
	public void testSetLightblueServiceURI() {
		command.setLightblueEntityName(legacyServiceURI);
		Assert.assertEquals(legacyServiceURI, command.getLightblueServiceURI());
	}

	@Test
	public void testGetLegacyServiceURI() {
		Assert.assertEquals(legacyServiceURI, command.getLegacyServiceURI());
	}

	@Test
	public void testSetLegacyServiceURI() {
		command.setLegacyEntityName(lightblueServiceURI);
		Assert.assertEquals(lightblueServiceURI, command.getLegacyServiceURI());
	}

	@Test
	public void testSetOverwriteLightblueDocuments() {
		command.setOverwriteLightblueDocuments(false);
		Assert.assertEquals(false, command.shouldOverwriteLightblueDocuments());
	}

	@Test
	public void testShouldOverwriteLightblueDocuments() {
		Assert.assertEquals(true, command.shouldOverwriteLightblueDocuments());
	}

	@Test
	public void testExecute() {
		command.setLightblueEntityVersion("1.0.0");
		command.setLightblueEntityName("country");
		command.setLegacyEntityVersion("1.0.0");
		command.setLegacyEntityVersion("country");
		command.execute();
	}

}
