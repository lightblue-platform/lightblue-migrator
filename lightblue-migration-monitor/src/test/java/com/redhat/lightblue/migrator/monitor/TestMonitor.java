package com.redhat.lightblue.migrator.monitor;

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource.LightblueTestMethods;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.migrator.MigrationConfiguration;
import com.redhat.lightblue.migrator.MigrationJob;

public class TestMonitor {

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

    @After
    public void after() throws Exception {
        lightblue.cleanupMongoCollections(
                "migrationConfig",
                MigrationJob.ENTITY_NAME);
    }

    private MigrationConfiguration generateMigrationConfiguration(String period) {
        MigrationConfiguration config = new MigrationConfiguration();
        config.setConsistencyCheckerName("test");
        config.setConfigurationName("fake");
        config.setSourceEntityName("fakeSourceEntity");
        config.setSourceEntityVersion("1.0.0");
        config.setDestinationEntityName("fakeDestinationEntity");
        config.setDestinationEntityVersion("1.0.0");
        config.setDestinationIdentityFields(Arrays.asList("_id"));
        config.setPeriod(period);

        return config;
    }

    private MigrationJob generateMigrationJob() {
        MigrationJob job = new MigrationJob();
        job.setConfigurationName("fake");
        job.setStatus("ready");
        job.setScheduledDate(new Date());

        return job;
    }

    @Test
    public void testRunCheck_NoConfigurations() throws Exception {
        lightblue.getLightblueClient(); //Needed to perform behind the scenes setup

        Monitor monitor = new Monitor(new MonitorConfiguration());
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                //Do nothing
            }

            @Override
            public void sendFailure(List<String> configurationsMissingJobs) {
                fail("Should not be a failure");
            }
        });
    }

    @Test
    public void testRunCheck_MissingJob() throws Exception {
        DataInsertRequest insert = new DataInsertRequest(MigrationConfiguration.ENTITY_NAME);
        insert.create(generateMigrationConfiguration("1 day"));
        insert.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insert);

        Monitor monitor = new Monitor(new MonitorConfiguration());
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                fail("Should be a failure");
            }

            @Override
            public void sendFailure(List<String> configurationsMissingJobs) {
                assertEquals(1, configurationsMissingJobs.size());
                assertEquals("fake", configurationsMissingJobs.get(0));
            }
        });
    }

    @Test
    public void testRunCheck_HasCurrentJob() throws Exception {
        DataInsertRequest insertConfigRequest = new DataInsertRequest(MigrationConfiguration.ENTITY_NAME);
        insertConfigRequest.create(generateMigrationConfiguration("1 day"));
        insertConfigRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertConfigRequest);

        DataInsertRequest insertJobRequest = new DataInsertRequest(MigrationJob.ENTITY_NAME);
        insertJobRequest.create(generateMigrationJob());
        insertJobRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertJobRequest);

        Monitor monitor = new Monitor(new MonitorConfiguration());
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                //Do nothing
            }

            @Override
            public void sendFailure(List<String> configurationsMissingJobs) {
                fail("Should not be a failure");
            }
        });
    }

}
