package com.redhat.lightblue.migrator.monitor;

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource.LightblueTestMethods;
import com.redhat.lightblue.migrator.MigrationConfiguration;
import com.redhat.lightblue.migrator.MigrationJob;

public abstract class AbstractMonitorTest {

    @ClassRule
    public static LightblueExternalResource lightblue = new LightblueExternalResource(new LightblueTestMethods() {

        @Override
        public JsonNode[] getMetadataJsonNodes() throws Exception {
            return new JsonNode[]{
                    loadJsonNode("migrationConfiguration.json"),
                    loadJsonNode("migrationJob.json")
                };
        }
    });

    @Before
    public void before() {
        lightblue.getLightblueClient(); //Needed to perform behind the scenes setup
    }

    @After
    public void after() throws Exception {
        lightblue.cleanupMongoCollections(
                "migrationConfig",
                MigrationJob.ENTITY_NAME);
    }

    protected MigrationConfiguration generateMigrationConfiguration(String configurationName) {
        MigrationConfiguration config = new MigrationConfiguration();
        config.setConsistencyCheckerName("test");
        config.setConfigurationName(configurationName);
        config.setSourceEntityName("fakeSourceEntity");
        config.setSourceEntityVersion("1.0.0");
        config.setDestinationEntityName("fakeDestinationEntity");
        config.setDestinationEntityVersion("1.0.0");
        config.setDestinationIdentityFields(Arrays.asList("_id"));

        return config;
    }

}
