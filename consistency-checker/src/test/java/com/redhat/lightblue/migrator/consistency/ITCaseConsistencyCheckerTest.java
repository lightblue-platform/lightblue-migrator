package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;
import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.client.expression.query.ValueQuery;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.migrator.test.model.Customer;

public class ITCaseConsistencyCheckerTest extends AbstractMigratorController {

    private final ConsistencyChecker consistencyChecker;
    private static boolean initialized = false;

    private static String versionMigrationJob;
    private static String versionMigrationConfiguration;
    private static String versionSourceCustomer;
    private static String versionDestinationCustomer;

    public ITCaseConsistencyCheckerTest() throws Exception {
        super();

        if (!initialized) {
            loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations.json");
            loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
            loadData("sourceCustomer", versionSourceCustomer, "./test/data/load-source-customers.json");
            initialized = true;
        }

        consistencyChecker = new ConsistencyChecker();
        //No need to set the client to getLightblueClient, migrator will load it from lightblue-client.properties
        consistencyChecker.setMigrationJobEntityVersion(versionMigrationJob);
        consistencyChecker.setMigrationConfigurationEntityVersion(versionMigrationConfiguration);
        consistencyChecker.setConsistencyCheckerName("continuum");
    }

    @Override
    protected JsonNode[] getMetadataJsonNodes() throws Exception {
        ObjectNode jsonMigrationJob = (ObjectNode) loadJsonNode("./migrationJob.json");
        ObjectNode jsonMigrationConfiguration = (ObjectNode) loadJsonNode("./migrationConfiguration.json");
        ObjectNode jsonSourceCustomer = (ObjectNode) loadJsonNode("./test/metadata/source-customer.json");
        ObjectNode jsonDestinationCustomer = (ObjectNode) loadJsonNode("./test/metadata/destination-customer.json");

        versionMigrationJob = parseEntityVersion(jsonMigrationJob);
        versionMigrationConfiguration = parseEntityVersion(jsonMigrationConfiguration);
        versionSourceCustomer = parseEntityVersion(jsonSourceCustomer);
        versionDestinationCustomer = parseEntityVersion(jsonDestinationCustomer);

        return new JsonNode[]{
                removeHooks(grantAnyoneAccess(jsonMigrationJob)),
                removeHooks(grantAnyoneAccess(jsonMigrationConfiguration)),
                jsonSourceCustomer,
                jsonDestinationCustomer
        };
    }

    @Test
    public void testGetJobConfigurations() {
        List<MigrationConfiguration> configs = consistencyChecker.getJobConfigurations();

        assertNotNull(configs);
        assertEquals(1, configs.size());
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

    @Test
    public void testRun() throws Exception {
        Thread consistencyThread = new Thread(consistencyChecker);
        try {
            consistencyThread.start();
            Thread.sleep(1000);
        } finally {
            consistencyThread.interrupt();
        }

        DataFindRequest findRequest = new DataFindRequest("destCustomer", versionDestinationCustomer);
        findRequest.where(ValueQuery.withValue("objectType = destCustomer"));
        findRequest.select(includeFieldRecursively("*"));

        Customer[] customers = getLightblueClient().data(findRequest, Customer[].class);

        assertNotNull(customers);
        assertEquals(5, customers.length);
    }
}
