package com.redhat.lightblue.migrator.facade;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.redhat.lightblue.migrator.facade.model.Country;

public class MethodCallStringifierTest {

    @Test
    public void testSimple() {
        Assert.assertEquals("blah(12, string)",
                ServiceFacade.methodCallToString("blah", new Object[]{12, "string"}));
    }

    @Test
    public void testListOfObjects() {
        List<Country> l = new ArrayList<>();
        l.add(new Country(1l, "PL"));
        l.add(new Country(2l, "CA"));

        Assert.assertEquals("blah([PL, CA], 12, string)",
                ServiceFacade.methodCallToString("blah", new Object[]{l, 12, "string"}));
    }

    @Test
    public void testListOfPrimitives() {
        List<Long> l = new ArrayList<>();
        l.add(1l);
        l.add(2l);

        Assert.assertEquals("blah([1, 2], 12, string)",
                ServiceFacade.methodCallToString("blah", new Object[]{l, 12, "string"}));
    }

    @Test
    public void testArrayOfPrimitives() {
        Long[] arr = new Long[]{1l, 2l};

        Assert.assertEquals("blah([1, 2], 12, string)",
                ServiceFacade.methodCallToString("blah", new Object[]{arr, 12, "string"}));
    }

    @Test
    public void testArrayOfObjects() {
        Country[] arr = new Country[]{new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertEquals("blah([PL, CA], 12, string)",
                ServiceFacade.methodCallToString("blah", new Object[]{arr, 12, "string"}));
    }

}
