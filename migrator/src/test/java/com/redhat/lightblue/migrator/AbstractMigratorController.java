package com.redhat.lightblue.migrator;

import java.util.Iterator;

import org.junit.BeforeClass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.test.utils.AbstractCRUDControllerWithRest;

public abstract class AbstractMigratorController extends AbstractCRUDControllerWithRest {

    @BeforeClass
    public static void prepareMetadataDatasources() {
        System.setProperty("mongo.datasource", "mongodata");
    }

    public AbstractMigratorController() throws Exception {
        super();
    }

    /**
     * Work around method until a way to pass in security access level is
     * found.
     */
    protected ObjectNode grantAnyoneAccess(ObjectNode node) {
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

    /**
     * Temporary method that will remove the hooks from  metadata. Ideally, this
     * method will go away once the test generic metadata supports arbitrary configurations.
     */
    protected ObjectNode removeHooks(ObjectNode node) {
        ObjectNode schema = (ObjectNode) node.get("entityInfo");
        schema.remove("hooks");
        return node;
    }

    /**
     *
     * @param node
     * @return the schema version for the passed in node.
     */
    protected String parseEntityVersion(ObjectNode node) {
        ObjectNode schema = (ObjectNode) node.get("schema");
        ObjectNode version = (ObjectNode) schema.get("version");
        return version.get("value").textValue();
    }

}
