package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ITCaseConsistencyCheckerTest extends AbstractMigratorController {

    public ITCaseConsistencyCheckerTest() throws Exception {
        super();

        loadData("migrationConfiguration", "1.0.0", "./test/data/load-migration-configurations.json");
        loadData("migrationJob", "1.0.0", "./test/data/load-migration-jobs.json");
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

    @Test
    public void test() {
        assertTrue(true);
    }

}
