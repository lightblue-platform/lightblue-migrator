package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ITCaseConsistencyCheckerTest extends AbstractMigratorController {

    private final ConsistencyChecker consistencyChecker;
    private static boolean initialized = false;

    public ITCaseConsistencyCheckerTest() throws Exception {
        super();

        if (!initialized) {
            loadData("migrationConfiguration", "1.0.0", "./test/data/load-migration-configurations.json");
            loadData("migrationJob", "1.0.0", "./test/data/load-migration-jobs.json");
            initialized = true;
        }

        consistencyChecker = new ConsistencyChecker();
        consistencyChecker.setClient(getLightblueClient());
        consistencyChecker.setMigrationJobEntityVersion(parseEntityVersion((ObjectNode) loadJsonNode("./migrationJob.json")));
    }

    @Override
    protected JsonNode[] getMetadataJsonNodes() throws Exception {
        return new JsonNode[]{
                grantAnyoneAccess((ObjectNode) loadJsonNode("./migrationJob.json")),
                grantAnyoneAccess((ObjectNode) loadJsonNode("./migrationConfiguration.json"))
        };
    }

    /**
     * Work around method until a way to pass in security access level is
     * found.
     */
    private JsonNode grantAnyoneAccess(ObjectNode node) {
        ObjectNode schema = (ObjectNode) node.get("schema");
        ObjectNode access = (ObjectNode) schema.get("access");
        Iterator<JsonNode> children = access.iterator();
        while (children.hasNext()) {
            ArrayNode child = (ArrayNode) children.next();
            child.removeAll();
            child.add("anyone");
        }

        return node;
    }

    private String parseEntityVersion(ObjectNode node) {
        ObjectNode schema = (ObjectNode) node.get("schema");
        ObjectNode version = (ObjectNode) schema.get("version");
        return version.get("value").textValue();
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
