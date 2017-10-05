package com.redhat.lightblue.migrator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CompareTest {

    @Test
    public void testExcludeSizeFields() throws Exception {
        ObjectNode n1 = JsonNodeFactory.instance.objectNode();
        ObjectNode n2 = JsonNodeFactory.instance.objectNode();
        n1.set("a", JsonNodeFactory.instance.numberNode(1));
        n2.set("a", JsonNodeFactory.instance.numberNode(1));
        n1.set("a#", JsonNodeFactory.instance.numberNode(1));
        List<Inconsistency> list = Utils.compareDocs(n1, n2, new ArrayList<String>(), false);
        Assert.assertTrue(list.isEmpty());
        list = Utils.compareDocs(n2, n1, new ArrayList<String>(), false);
        Assert.assertTrue(list.isEmpty());
    }

    @Test
    public void testSimpleDiff() throws Exception {
        ObjectNode n1 = JsonNodeFactory.instance.objectNode();
        ObjectNode n2 = JsonNodeFactory.instance.objectNode();
        ArrayNode a1, a2;
        n1.set("arr", a1 = JsonNodeFactory.instance.arrayNode());
        n2.set("arr", a2 = JsonNodeFactory.instance.arrayNode());

        ObjectNode x1 = JsonNodeFactory.instance.objectNode();
        x1.set("x", JsonNodeFactory.instance.textNode("x"));
        x1.set("y", JsonNodeFactory.instance.textNode("y"));
        a1.add(x1);

        ObjectNode x2 = JsonNodeFactory.instance.objectNode();
        x2.set("x", JsonNodeFactory.instance.textNode("y"));
        x2.set("y", JsonNodeFactory.instance.textNode("y"));
        a2.add(x2);

        List<Inconsistency> list = Utils.compareDocs(n1, n2, new ArrayList<String>(), false);
        System.out.println(list);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals("arr.0.x", list.get(0).getField());
    }

    @Test
    public void testDatesWhichDifferByMS() {
        ObjectNode n1 = JsonNodeFactory.instance.objectNode();
        ObjectNode n2 = JsonNodeFactory.instance.objectNode();

        n1.put("timestamp", "20071203T10:15:30.000-0400");
        n2.put("timestamp", "20071203T10:15:30.015-0400");

        List<Inconsistency> list = Utils.compareDocs(n1, n2, new ArrayList<String>(), false);
        Assert.assertEquals(1, list.size());

        list = Utils.compareDocs(n1, n2, new ArrayList<String>(), true);
        Assert.assertEquals(0, list.size());

        n1.put("timestamp", "20071203T10:15:30.000-0400");
        n2.put("timestamp", "20071203T10:15:29.999-0400");

        list = Utils.compareDocs(n1, n2, new ArrayList<String>(), true);
        Assert.assertEquals(1, list.size());
    }

}
