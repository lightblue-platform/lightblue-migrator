package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ITCaseConsistencyCheckerTest extends AbstractMigratorController {

    private final ConsistencyChecker consistencyChecker;
    private static boolean initialized = false;

    private static String versionMigrationJob;
    private static String versionMigrationConfiguration;

    public ITCaseConsistencyCheckerTest() throws Exception {
        super();

        if (!initialized) {
            loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations.json");
            loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
            initialized = true;
        }

        consistencyChecker = new ConsistencyChecker();
        consistencyChecker.setClient(getLightblueClient());
        consistencyChecker.setMigrationJobEntityVersion(versionMigrationJob);
    }

    @Override
    protected JsonNode[] getMetadataJsonNodes() throws Exception {
        ObjectNode jsonMigrationJob = (ObjectNode) loadJsonNode("./migrationJob.json");
        ObjectNode jsonMigrationConfiguration = (ObjectNode) loadJsonNode("./migrationConfiguration.json");

        versionMigrationJob = parseEntityVersion(jsonMigrationJob);
        versionMigrationConfiguration = parseEntityVersion(jsonMigrationConfiguration);

        return new JsonNode[]{
                grantAnyoneAccess(jsonMigrationJob),
                grantAnyoneAccess(jsonMigrationConfiguration)
        };
    }

    @Test
    public void testGetMigrationJobs() {
        MigrationConfiguration config = new MigrationConfiguration();
        config.setConfigurationName("customers");
        List<MigrationJob> jobs = consistencyChecker.getMigrationJobs(config);

        assertNotNull(jobs);
        assertEquals(1, jobs.size());
    }

    /**
     * Assert that null is returned when there is not a next available job.
     */
    @Test
    public void testGetNextAvailableJob() {
        MigrationJob job = consistencyChecker.getNextAvailableJob();
        assertNull(job);
    }

}
