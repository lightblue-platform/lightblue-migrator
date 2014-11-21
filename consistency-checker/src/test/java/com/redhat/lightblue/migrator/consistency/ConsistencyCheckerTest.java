package com.redhat.lightblue.migrator.consistency;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;

public class ConsistencyCheckerTest {

	private String checkerName = "testChecker";
	private String hostname = "http://lightblue.io";
	private String ipAddress = "127.0.0.1";
	private String serviceURI = "http://demo.lightblue.io/rest/data/";
	
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

	ConsistencyChecker checker;
	
	LightblueClient client;
	
	@Before
	public void setUp() throws Exception {
		checker = new ConsistencyChecker();
		checker.setCheckerName(checkerName);
		checker.setHostname(hostname);
		checker.setIpAddress(ipAddress);
		checker.setServiceURI(legacyServiceURI);
		client = new LightblueHttpClient();
		checker.setClient(client);
	}

	@Test
	public void testGetCheckerName() {
		Assert.assertEquals(checkerName, checker.getCheckerName());
	}

	@Test
	public void testSetCheckerName() {
		checker.setCheckerName(hostname);
		Assert.assertEquals(hostname, checker.getCheckerName());
	}

	@Test
	public void testGetHostName() {
		Assert.assertEquals(hostname, checker.getHostname());
	}

	@Test
	public void testSetHostName() {
		checker.setHostname(checkerName);
		Assert.assertEquals(checkerName, checker.getHostname());
	}

	@Test
	public void testGetIpAddress() {
		Assert.assertEquals(ipAddress, checker.getIpAddress());
	}

	@Test
	public void testSetIpAddress() {
		checker.setIpAddress(hostname);
		Assert.assertEquals(hostname, checker.getIpAddress());
	}
	
	@Test
	public void testGetServiceURI() {
		Assert.assertEquals(serviceURI, checker.getServiceURI());
	}

	@Test
	public void testSetServiceURI() {
		checker.setServiceURI(ipAddress);
		Assert.assertEquals(ipAddress, checker.getServiceURI());
	}
}
