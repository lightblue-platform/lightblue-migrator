package com.redhat.lightblue.migrator.monitor.HIR;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.migrator.MigrationJob;
import com.redhat.lightblue.migrator.MigrationJob.JobExecution;
import com.redhat.lightblue.migrator.monitor.AbstractMonitorTest;
import com.redhat.lightblue.migrator.monitor.MonitorConfiguration;
import com.redhat.lightblue.migrator.monitor.Notifier;

public class TestHIRMonitor extends AbstractMonitorTest {

    protected MigrationJob generateMigrationJob(Date actualEndDate, int processedCount, int overwrittenCount) {
        MigrationJob job = new MigrationJob();
        job.setConfigurationName("fake");
        job.setStatus("completed");
        job.setGenerated(true);
        job.setScheduledDate(new Date());

        JobExecution e = new JobExecution();
        e.setActualStartDate(new Date());
        e.setActualEndDate(actualEndDate);
        e.setProcessedDocumentCount(processedCount);
        e.setOverwrittenDocumentCount(overwrittenCount);
        e.setStatus("fakeStatus");
        e.setHostName("fakeHost");
        e.setOwnerName("fakeOwner");
        job.setJobExecutions(Arrays.asList(e));

        return job;
    }

    @Test
    public void testRunCheck_ThresholdNotMet() throws LightblueException {
        DataInsertRequest insertJobRequest = new DataInsertRequest(MigrationJob.ENTITY_NAME);
        insertJobRequest.create(generateMigrationJob(new Date(), 5, 0));
        insertJobRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertJobRequest);

        MonitorConfiguration cfg = new MonitorConfiguration();
        cfg.setConfigurationName("fake");
        cfg.setThreshold(1);
        HIRMonitor monitor = new HIRMonitor(cfg);
        monitor.runCheck(new Notifier() {

            @Override
            public void sendFailure(String message) {
                fail("Should not be a failure");
            }

            @Override
            public void sendSuccess() {
                //Do nothing
            }
        });
    }

    @Test
    public void testRunCheck_ThresholdExceeded() throws LightblueException {
        DataInsertRequest insertJobRequest = new DataInsertRequest(MigrationJob.ENTITY_NAME);
        insertJobRequest.create(generateMigrationJob(new Date(), 5, 5));
        insertJobRequest.returns(Projection.excludeFieldRecursively("*"));
        lightblue.getLightblueClient().data(insertJobRequest);

        MonitorConfiguration cfg = new MonitorConfiguration();
        cfg.setConfigurationName("fake");
        cfg.setThreshold(1);
        HIRMonitor monitor = new HIRMonitor(cfg);
        monitor.runCheck(new Notifier() {

            @Override
            public void sendFailure(String message) {
                //Do nothing
            }

            @Override
            public void sendSuccess() {
                fail("Should be a failure");
            }
        });
    }

}
