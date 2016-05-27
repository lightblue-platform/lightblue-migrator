package com.redhat.lightblue.migrator;

import java.util.*;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;

public class MainCfgTest {

    @Test
    public void testMainCfg() throws Exception {
        Properties p=MainConfiguration.processArguments(new String[] {
                "--config=cfg",
                "--hostname=host",
                "--name=name"
            });
        System.out.println(p);
        Assert.assertEquals("name",p.getProperty("name"));
        Assert.assertEquals("cfg",p.getProperty("config"));
        Assert.assertEquals("host",p.getProperty("hostname"));
    }
}
