package com.redhat.lightblue.migrator.facade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import jiff.JsonDiff;

/**
 * Test consistency check time for a list of objects, ignoring order.
 *
 * @author mpatercz
 *
 */
public class ConsistencyCheckPerforformanceTest {

    private static final Logger log = LoggerFactory.getLogger(ConsistencyCheckPerforformanceTest.class);

    int FOO_COUNT = 10000;

    List<Foo> fooList = new ArrayList<Foo>();

    Random random = new Random();

    class Foo {
        public Foo(String strField1, String strField2, Long longField3) {
            super();
            this.strField1 = strField1;
            this.strField2 = strField2;
            this.longField3 = longField3;
        }
        public String strField1;
        public String strField2;
        public Long longField3;
    }

    private Foo generateRandomFoo() {
        return new Foo(UUID.randomUUID()+" "+UUID.randomUUID(), UUID.randomUUID()+" "+UUID.randomUUID(), random.nextLong());
    }

    public ConsistencyCheckPerforformanceTest() {
        log.info("Generating "+FOO_COUNT+" Foo objects");
        for (int i=0;i<FOO_COUNT;i++) {
            fooList.add(generateRandomFoo());
        }
        log.info("Generation complete");
    }

    /**
     * On my machine: Consistency check took: 432 ms.
     *
     */
    @Test
    public void testConsistencyCheckerPerformance() {
        ConsistencyChecker c = new ConsistencyChecker("Bar");
        Timer t = new Timer("checkConsistency");
        //Assert.assertTrue(c.checkConsistency(fooList, fooList));
        Assert.assertTrue(c.checkConsistency("foo", "foo"));
        long tookMs = t.complete();
        log.info("Total consistency check (including conversion to json) took "+tookMs+"ms");
    }

    /**
     * On my machine: Jiff consistency check took 113ms.
     *
     * @throws IOException
     */
    @Test
    public void testJiffPerformance() throws IOException {

        String jsonStr = new ObjectMapper().writeValueAsString(fooList);

        log.info("Json str size: "+jsonStr.length()/1024+"kB");

        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);

        Timer t = new Timer("computeDiff");
        Assert.assertTrue(diff.computeDiff(jsonStr, jsonStr).isEmpty());
        long tookMs = t.complete();
        log.info("Jiff consistency check took "+tookMs+"ms");
    }

}
