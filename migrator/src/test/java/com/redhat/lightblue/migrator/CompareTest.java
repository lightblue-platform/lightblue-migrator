package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;


public class CompareTest {

    @Test
    public void testExcludeSizeFields() throws Exception {
        ObjectNode n1=JsonNodeFactory.instance.objectNode();
        ObjectNode n2=JsonNodeFactory.instance.objectNode();
        n1.set("a",JsonNodeFactory.instance.numberNode(1));
        n2.set("a",JsonNodeFactory.instance.numberNode(1));
        n1.set("a#",JsonNodeFactory.instance.numberNode(1));        
        List<Inconsistency> list=Utils.compareDocs(n1,n2,new ArrayList<String>());
        Assert.assertTrue(list.isEmpty());
        list=Utils.compareDocs(n2,n1,new ArrayList<String>());
        Assert.assertTrue(list.isEmpty());
    }
    
}
