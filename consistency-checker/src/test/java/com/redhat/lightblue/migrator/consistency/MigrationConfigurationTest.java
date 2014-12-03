package com.redhat.lightblue.migrator.consistency;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MigrationConfigurationTest {

    private static final String hostName = "lightblue.io";
    private static final int threadCount = 63;

    private static final List<String> lightblueEntityKeyFields = Arrays.asList("id", "name");
    ;
	private static final String lightblueEntityTimestampField = "lastUpdateDate";
    private static final List<String> legacyEntityKeyFields = Arrays.asList("id", "name");
    ;;
	private static final String legacyEntityTimestampField = "lastUpdateDate";

    private static final String lightblueEntityVersion = "lightblueEntityVersion";
    private static final String lightblueEntityName = "lightblueEntityName";
    private static final String legacyEntityVersion = "legacyEntityVersion";
    private static final String legacyEntityName = "legacyEntityName";

    MigrationConfiguration configuration;

    @Before
    public void setUp() throws Exception {
        configuration = new MigrationConfiguration();
        configuration.setHostName(hostName);
        configuration.setThreadCount(threadCount);
        configuration.setOverwriteDestinationDocuments(true);
        configuration.setDestinationEntityKeyFields(lightblueEntityKeyFields);
        configuration.setDestinationEntityTimestampField(lightblueEntityTimestampField);
        configuration.setSourceEntityKeyFields(legacyEntityKeyFields);
        configuration.setSourceEntityTimestampField(legacyEntityTimestampField);
        configuration.setSourceEntityName(legacyEntityName);
        configuration.setSourceEntityVersion(legacyEntityVersion);
        configuration.setDestinationEntityName(lightblueEntityName);
        configuration.setDestinationEntityVersion(lightblueEntityVersion);
        configuration.setOverwriteDestinationDocuments(true);
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
        Assert.assertEquals(lightblueEntityName, configuration.getDestinationEntityName());
    }

    @Test
    public void testSetLightblueEntityName() {
        configuration.setDestinationEntityName(legacyEntityName);
        Assert.assertEquals(legacyEntityName, configuration.getDestinationEntityName());
    }

    @Test
    public void testGetLightblueEntityVersion() {
        Assert.assertEquals(lightblueEntityVersion, configuration.getDestinationEntityVersion());
    }

    @Test
    public void testSetLightblueEntityVersion() {
        configuration.setDestinationEntityVersion(lightblueEntityName);
        Assert.assertEquals(lightblueEntityName, configuration.getDestinationEntityVersion());
    }

    @Test
    public void testGetLightblueEntityKeyFields() {
        Assert.assertEquals(lightblueEntityKeyFields, configuration.getDestinationEntityKeyFields());
    }

    @Test
    public void testSetLightblueEntityKeyFields() {
        configuration.setDestinationEntityKeyFields(legacyEntityKeyFields);
        Assert.assertEquals(legacyEntityKeyFields, configuration.getDestinationEntityKeyFields());
    }

    @Test
    public void testGetLightblueEntityTimestampField() {
        Assert.assertEquals(lightblueEntityTimestampField, configuration.getDestinationEntityTimestampField());
    }

    @Test
    public void testSetLightblueEntityTimestampField() {
        configuration.setDestinationEntityTimestampField(legacyEntityTimestampField);
        Assert.assertEquals(legacyEntityTimestampField, configuration.getDestinationEntityTimestampField());
    }

    @Test
    public void testGetLegacyEntityName() {
        Assert.assertEquals(legacyEntityName, configuration.getSourceEntityName());
    }

    @Test
    public void testSetLegacyEntityName() {
        configuration.setSourceEntityName(lightblueEntityName);
        Assert.assertEquals(lightblueEntityName, configuration.getSourceEntityName());
    }

    @Test
    public void testGetLegacyEntityVersion() {
        Assert.assertEquals(legacyEntityVersion, configuration.getSourceEntityVersion());
    }

    @Test
    public void testSetLegacyEntityVersion() {
        configuration.setSourceEntityVersion(lightblueEntityVersion);
        Assert.assertEquals(lightblueEntityVersion, configuration.getSourceEntityVersion());
    }

    @Test
    public void testGetLegacyEntityKeyFields() {
        Assert.assertEquals(legacyEntityKeyFields, configuration.getSourceEntityKeyFields());
    }

    @Test
    public void testSetLegacyEntityKeyFields() {
        configuration.setSourceEntityKeyFields(legacyEntityKeyFields);
        Assert.assertEquals(legacyEntityKeyFields, configuration.getSourceEntityKeyFields());
    }

    @Test
    public void testGetLegacyEntityTimestampField() {
        Assert.assertEquals(legacyEntityTimestampField, configuration.getSourceEntityTimestampField());
    }

    @Test
    public void testSetLegacyEntityTimestampField() {
        configuration.setSourceEntityTimestampField(legacyEntityTimestampField);
        Assert.assertEquals(legacyEntityTimestampField, configuration.getSourceEntityTimestampField());
    }

    @Test
    public void testSetOverwriteLightblueDocuments() {
        configuration.setOverwriteDestinationDocuments(false);
        Assert.assertEquals(false, configuration.shouldOverwriteDestinationDocuments());
    }

    @Test
    public void testShouldOverwriteLightblueDocuments() {
        Assert.assertEquals(true, configuration.shouldOverwriteDestinationDocuments());
    }

}
