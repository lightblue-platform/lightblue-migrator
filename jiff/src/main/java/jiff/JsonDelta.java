package jiff;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Represents a difference between two instances of a field
 *
 * @author bserdar
 */
public class JsonDelta {

    private final String field;
    private final JsonNode node1;
    private final JsonNode node2;

    public JsonDelta(String field,
                     JsonNode node1,
                     JsonNode node2) {
        this.field=field;
        this.node1=node1;
        this.node2=node2;
    }

    public String getField() {
        return field;
    }

    public JsonNode getNode1() {
        return node1;
    }

    public JsonNode getNode2() {
        return node2;
    }

    @Override
    public String toString() {
        return field+"("+describe(node1)+" != "+ describe(node2)+")";
    }

    private String describe(JsonNode node) {
        if(node==null)
            return "null";
        else if(node instanceof ObjectNode)
            return "ObjectNode";
        else if(node instanceof ArrayNode)
            return "ArrayNode";
        else
            return node.toString();
    }
}
