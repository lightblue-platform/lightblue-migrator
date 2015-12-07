package com.redhat.lightblue.migrator.facade;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class TimeoutConfigurationTest {

    @Test
    public void testDefault() {

        TimeoutConfiguration t = new TimeoutConfiguration(1000, "FooService", new Properties());

        Assert.assertEquals(1000, t.getTimeoutMS("bar"));
    }

    @Test
    public void testMethod() {
        Properties p = new Properties();
        p.setProperty(TimeoutConfiguration.TIMEOUT_CONFIG_PREFIX+"FooService.fooBar", "5000");
        p.setProperty(TimeoutConfiguration.TIMEOUT_CONFIG_PREFIX+"BarService.fooBar", "10000");

        TimeoutConfiguration t = new TimeoutConfiguration(2000, "FooService", p);

        // default
        Assert.assertEquals(2000, t.getTimeoutMS("barFoo"));
        // method specific
        Assert.assertEquals(5000, t.getTimeoutMS("fooBar"));
    }

}
