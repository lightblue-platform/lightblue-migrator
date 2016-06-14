package com.redhat.lightblue.migrator.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class TestMonitorConfiguration {

    @Test
    public void testProcessArguments_NoProperties(){
        assertNull(MonitorConfiguration.processArguments(new String[]{}));
        assertNull(MonitorConfiguration.processArguments(null));
    }

    @Test
    public void testProcessArguments_LightblueClientProperties() {
        String clientPropPath = "/some/path/lightblue-client.properties";

        MonitorConfiguration cfg = MonitorConfiguration.processArguments(
                new String[]{"-c", clientPropPath, 
                        "-j", JobType.NEW_MIGRATION_PERIODS.toString()});

        assertEquals(clientPropPath, cfg.getClientConfig());
    }

    @Test
    public void testProcessArguments_Periods() {
        Integer periods = 2;

        MonitorConfiguration cfg = MonitorConfiguration.processArguments(
                new String[]{"-c", "/some/path/lightblue-client.properties", 
                        "-j", JobType.NEW_MIGRATION_PERIODS.toString(), 
                        "-p", periods.toString()});

        assertEquals(periods, cfg.getPeriods());
    }

}
