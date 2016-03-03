package jiff;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Implementations of this interface compares two JSON nodes, and
 * records the differences into a differences list
 *
 * @author bserdar
 */
public interface JsonComparator {
    /**
     * Returns true if there is a difference. 
     *
     * @param delta Record differences here
     * @param context The field name being compared
     * @param node1 First node
     * @param node2 Second node
     */
    boolean compare(List<JsonDelta> delta,List<String> context,JsonNode node1,JsonNode node2);
}
