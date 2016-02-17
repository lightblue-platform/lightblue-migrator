package com.redhat.lightblue.migrator.facade;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.redhat.lightblue.migrator.facade.ServiceFacade.FacadeOperation;

public class TimeoutConfigurationTest {

    @Test
    public void testDefault() {

        TimeoutConfiguration t = new TimeoutConfiguration(1000, "FooService", new Properties());

        Assert.assertEquals(1000, t.getTimeoutMS("bar", FacadeOperation.READ));
        Assert.assertEquals(1000, t.getTimeoutMS("bar", FacadeOperation.WRITE));
    }

    @Test
    public void testMethod() {
        Properties p = new Properties();
        p.setProperty(TimeoutConfiguration.TIMEOUT_CONFIG_PREFIX+"FooService.fooBar", "5000");
        p.setProperty(TimeoutConfiguration.TIMEOUT_CONFIG_PREFIX+"BarService.fooBar", "10000");

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
        p.setProperty(TimeoutConfiguration.TIMEOUT_CONFIG_PREFIX+"FooService.WRITE", "5000");
        p.setProperty(TimeoutConfiguration.TIMEOUT_CONFIG_PREFIX+"FooService.READ", "2000");
        p.setProperty(TimeoutConfiguration.TIMEOUT_CONFIG_PREFIX+"FooService.fooBar", "10000");

        TimeoutConfiguration t = new TimeoutConfiguration(2000, "FooService", p);


        Assert.assertEquals(2000, t.getTimeoutMS("foo", FacadeOperation.READ));
        Assert.assertEquals(5000, t.getTimeoutMS("bar", FacadeOperation.WRITE));
        // explicit configuration takes precedence
        Assert.assertEquals(10000, t.getTimeoutMS("fooBar", FacadeOperation.WRITE));
        Assert.assertEquals(10000, t.getTimeoutMS("fooBar", FacadeOperation.READ));
    }

}
