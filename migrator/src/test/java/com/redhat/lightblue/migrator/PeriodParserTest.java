package com.redhat.lightblue.migrator;

import org.junit.Test;
import org.junit.Assert;

public class PeriodParserTest {

    @Test
    public void periodParserTest() {
        //Assert.assertEquals(100l,ConsistencyCheckerController.parsePeriod("100"));
        Assert.assertEquals(100l, ConsistencyCheckerController.parsePeriod("100 milliseconds"));
        //Assert.assertEquals(100l,ConsistencyCheckerController.parsePeriod("100 msec"));
        //Assert.assertEquals(100l,ConsistencyCheckerController.parsePeriod("100 msecs"));
        Assert.assertEquals(100l * 1000l, ConsistencyCheckerController.parsePeriod("100 seconds"));
        //Assert.assertEquals(100l*1000l,ConsistencyCheckerController.parsePeriod("100 s"));
        //Assert.assertEquals(100l*1000l,ConsistencyCheckerController.parsePeriod("100 sec"));
        Assert.assertEquals(100l * 1000l * 60l, ConsistencyCheckerController.parsePeriod("100 minutes"));
        //Assert.assertEquals(100l*1000l*60l,ConsistencyCheckerController.parsePeriod("100 min"));
        Assert.assertEquals(100l * 1000l * 60l * 60l, ConsistencyCheckerController.parsePeriod("100 hours"));
        //Assert.assertEquals(100l*1000l*60l*60l,ConsistencyCheckerController.parsePeriod("100h"));
        Assert.assertEquals(100l * 1000l * 60l * 60l * 24l, ConsistencyCheckerController.parsePeriod("100 days"));
    }
}
