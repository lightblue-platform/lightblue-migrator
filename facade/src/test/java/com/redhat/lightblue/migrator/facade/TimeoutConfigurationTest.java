package com.redhat.lightblue.migrator.facade;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.redhat.lightblue.migrator.facade.ServiceFacade.FacadeOperation;

public class TimeoutConfigurationTest {

    String TIMEOUT_CONFIG_PREFIX = TimeoutConfiguration.CONFIG_PREFIX+TimeoutConfiguration.Type.timeout+".";
    String SLOWWARNING_CONFIG_PREFIX = TimeoutConfiguration.CONFIG_PREFIX+TimeoutConfiguration.Type.slowwarning+".";

    @Test
    public void testDefault() {

        TimeoutConfiguration t = new TimeoutConfiguration(1000, "FooService", new Properties());

        Assert.assertEquals(1000, t.getTimeoutMS("bar", FacadeOperation.READ));
        Assert.assertEquals(1000, t.getTimeoutMS("bar", FacadeOperation.WRITE));
    }

    @Test
    public void testMethod() {
        Properties p = new Properties();
        p.setProperty(TIMEOUT_CONFIG_PREFIX + "FooService.fooBar", "5000");
        p.setProperty(TIMEOUT_CONFIG_PREFIX + "BarService.fooBar", "10000");

        TimeoutConfiguration t = new TimeoutConfiguration(2000, "FooService", p);

        // default
        Assert.assertEquals(2000, t.getTimeoutMS("barFoo", FacadeOperation.READ));
        Assert.assertEquals(2000, t.getTimeoutMS("barFoo", FacadeOperation.WRITE));
        // method specific
        Assert.assertEquals(5000, t.getTimeoutMS("fooBar", FacadeOperation.READ));
        Assert.assertEquals(5000, t.getTimeoutMS("fooBar", FacadeOperation.WRITE));
    }

    @Test
    public void testOperation() {
        Properties p = new Properties();
        p.setProperty(TIMEOUT_CONFIG_PREFIX + "FooService.WRITE", "5000");
        p.setProperty(TIMEOUT_CONFIG_PREFIX + "FooService.READ", "2000");
        p.setProperty(TIMEOUT_CONFIG_PREFIX + "FooService.fooBar", "10000");

        TimeoutConfiguration t = new TimeoutConfiguration(2000, "FooService", p);

        Assert.assertEquals(2000, t.getTimeoutMS("foo", FacadeOperation.READ));
        Assert.assertEquals(5000, t.getTimeoutMS("bar", FacadeOperation.WRITE));
        // explicit configuration takes precedence
        Assert.assertEquals(10000, t.getTimeoutMS("fooBar", FacadeOperation.WRITE));
        Assert.assertEquals(10000, t.getTimeoutMS("fooBar", FacadeOperation.READ));
    }

    @Test
    public void testSlowWarning() {
        Properties p = new Properties();

        p.setProperty(TIMEOUT_CONFIG_PREFIX + "FooService.WRITE", "5001");
        p.setProperty(TIMEOUT_CONFIG_PREFIX + "FooService.READ", "2001");
        p.setProperty(TIMEOUT_CONFIG_PREFIX + "FooService.fooBar", "10001");

        p.setProperty(SLOWWARNING_CONFIG_PREFIX + "FooService.WRITE", "5000");
        p.setProperty(SLOWWARNING_CONFIG_PREFIX + "FooService.READ", "2000");
        p.setProperty(SLOWWARNING_CONFIG_PREFIX + "FooService.fooBar", "10000");

        TimeoutConfiguration t = new TimeoutConfiguration(2000, "FooService", p);

        Assert.assertEquals(2000, t.getSlowWarningMS("foo", FacadeOperation.READ));
        Assert.assertEquals(5000, t.getSlowWarningMS("bar", FacadeOperation.WRITE));
        // explicit configuration takes precedence
        Assert.assertEquals(10000, t.getSlowWarningMS("fooBar", FacadeOperation.WRITE));
        Assert.assertEquals(10000, t.getSlowWarningMS("fooBar", FacadeOperation.READ));
        Assert.assertEquals(10001, t.getTimeoutMS("fooBar", FacadeOperation.READ));

        t = new TimeoutConfiguration(2000, "BarService", p);

        Assert.assertEquals(2000, t.getSlowWarningMS("foo", FacadeOperation.READ));
        Assert.assertEquals(2000, t.getSlowWarningMS("bar", FacadeOperation.WRITE));
        Assert.assertEquals(2000, t.getSlowWarningMS("fooBar", FacadeOperation.WRITE));
        Assert.assertEquals(2000, t.getSlowWarningMS("fooBar", FacadeOperation.READ));
        Assert.assertEquals(2000, t.getTimeoutMS("fooBar", FacadeOperation.READ));
    }

}
