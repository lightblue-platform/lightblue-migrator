package com.redhat.lightblue.migrator.monitor.NMP;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.migrator.MigrationConfiguration;
import com.redhat.lightblue.migrator.MigrationJob;
import com.redhat.lightblue.migrator.monitor.AbstractMonitorTest;
import com.redhat.lightblue.migrator.monitor.MonitorConfiguration;
import com.redhat.lightblue.migrator.monitor.Notifier;

public class TestNMPMonitor extends AbstractMonitorTest {

    @Override
    protected MigrationConfiguration generateMigrationConfiguration(String period) {
        MigrationConfiguration config = super.generateMigrationConfiguration("fake");
        config.setPeriod(period);

        return config;
    }

    protected MigrationJob generateMigrationJob(Date scheduledDate) {
        MigrationJob job = new MigrationJob();
        job.setConfigurationName("fake");
        job.setStatus("ready");
        job.setScheduledDate(scheduledDate);

        return job;
    }

    @Test
    public void testRunCheck_NoConfigurations() throws Exception {
        NMPMonitor monitor = new NMPMonitor(new MonitorConfiguration());
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                //Do nothing
            }

            @Override
            public void sendFailure(String message) {
                fail("Should not be a failure");
            }

            @Override
            public void sendError(String message) {
                fail("Should not be a error");
            }
        });
    }

    @Test
    public void testRunCheck_MissingJob() throws Exception {
        DataInsertRequest insert = new DataInsertRequest(MigrationConfiguration.ENTITY_NAME);
        insert.create(generateMigrationConfiguration("1 day"));
        insert.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insert);

        NMPMonitor monitor = new NMPMonitor(new MonitorConfiguration());
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                fail("Should be a failure");
            }

            @Override
            public void sendFailure(String message) {
                assertTrue(message.contains("fake"));
            }

            @Override
            public void sendError(String message) {
                fail("Should not be a error");
            }
        });
    }

    @Test
    public void testRunCheck_HasCurrentJob() throws Exception {
        DataInsertRequest insertConfigRequest = new DataInsertRequest(MigrationConfiguration.ENTITY_NAME);
        insertConfigRequest.create(generateMigrationConfiguration("1 day"));
        insertConfigRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertConfigRequest);

        DataInsertRequest insertJobRequest = new DataInsertRequest(MigrationJob.ENTITY_NAME);
        insertJobRequest.create(generateMigrationJob(new Date()));
        insertJobRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertJobRequest);

        NMPMonitor monitor = new NMPMonitor(new MonitorConfiguration());
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                //Do nothing
            }

            @Override
            public void sendFailure(String message) {
                fail("Should not be a failure");
            }

            @Override
            public void sendError(String message) {
                fail("Should not be a error");
            }
        });
    }

    @Test
    public void testRunCheck_Check2PeriodsAgo_Success() throws Exception {
        DataInsertRequest insertConfigRequest = new DataInsertRequest(MigrationConfiguration.ENTITY_NAME);
        insertConfigRequest.create(generateMigrationConfiguration("1 day"));
        insertConfigRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertConfigRequest);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        DataInsertRequest insertJobRequest = new DataInsertRequest(MigrationJob.ENTITY_NAME);
        insertJobRequest.create(generateMigrationJob(cal.getTime()));
        insertJobRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertJobRequest);

        MonitorConfiguration cfg = new MonitorConfiguration();
        cfg.setPeriods(2);
        NMPMonitor monitor = new NMPMonitor(cfg);
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                //Do nothing
            }

            @Override
            public void sendFailure(String message) {
                fail("Should not be a failure");
            }

            @Override
            public void sendError(String message) {
                fail("Should not be a error");
            }
        });
    }

    @Test
    public void testRunCheck_Check2PeriodsAgo_Failure() throws Exception {
        DataInsertRequest insertConfigRequest = new DataInsertRequest(MigrationConfiguration.ENTITY_NAME);
        insertConfigRequest.create(generateMigrationConfiguration("1 day"));
        insertConfigRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertConfigRequest);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        DataInsertRequest insertJobRequest = new DataInsertRequest(MigrationJob.ENTITY_NAME);
        insertJobRequest.create(generateMigrationJob(cal.getTime()));
        insertJobRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertJobRequest);

        MonitorConfiguration cfg = new MonitorConfiguration();
        cfg.setPeriods(1);
        NMPMonitor monitor = new NMPMonitor(cfg);
        monitor.runCheck(new Notifier() {

            @Override
            public void sendSuccess() {
                fail("Should be a failure");
            }

            @Override
            public void sendFailure(String message) {
                //Do nothing
            }

            @Override
            public void sendError(String message) {
                fail("Should not be a error");
            }
        });
    }

}
