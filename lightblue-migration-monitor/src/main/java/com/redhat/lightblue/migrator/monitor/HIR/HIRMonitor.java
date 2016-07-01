package com.redhat.lightblue.migrator.monitor.HIR;

import org.joda.time.DateTime;

import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.migrator.MigrationJob;
import com.redhat.lightblue.migrator.MigrationJob.JobExecution;
import com.redhat.lightblue.migrator.monitor.JobType;
import com.redhat.lightblue.migrator.monitor.Monitor;
import com.redhat.lightblue.migrator.monitor.MonitorConfiguration;
import com.redhat.lightblue.migrator.monitor.Notifier;

/** Monitor implementation for {@link JobType#HIGH_INCONSISTENCY_RATE} */
public class HIRMonitor extends Monitor {

    public HIRMonitor(MonitorConfiguration monitorCfg) {
        super(monitorCfg);
    }

    @Override
    protected void doRunCheck(final Notifier... notifiers) throws LightblueException {
        double inconsistencyRate = calculateInconsistencyRate(monitorCfg.getConfigurationName(), 1);

        if (inconsistencyRate >= monitorCfg.getThreshold()) {
            String message = "Migration " + monitorCfg.getConfigurationName() 
                + " has a high inconsistency rate of " + inconsistencyRate + "%";
            onFailure(message, notifiers);
        } else {
            onSuccess(notifiers);
        }
    }

    private double calculateInconsistencyRate(String configurationName, int days) throws LightblueException {
        DataFindRequest findJobs = new DataFindRequest(MigrationJob.ENTITY_NAME);
        findJobs.where(
            Query.and(
                Query.withValue("configurationName", Query.eq, configurationName),
                Query.withValue("status", Query.eq, "completed"),
                Query.withValue("jobExecutions.*.actualEndDate", Query.gte, new DateTime().minusDays(days).toDate()),
                Query.withValue("generated", Query.eq, true)
            )
        );
        findJobs.select(
            Projection.includeField("jobExecutions.*.processedDocumentCount"),
            Projection.includeField("jobExecutions.*.overwrittenDocumentCount")
        );

        MigrationJob[] jobs = lightblueClient.data(findJobs, MigrationJob[].class);

        double processedDocumentCount = 0;
        double overwrittenDocumentCount = 0;
        for (MigrationJob job : jobs) {
            JobExecution execution = job.getJobExecutions().get(0);

            processedDocumentCount += execution.getProcessedDocumentCount();
            overwrittenDocumentCount += execution.getOverwrittenDocumentCount();
        }

        double percent = 0;
        if(processedDocumentCount != 0){
            percent = 100 * (overwrittenDocumentCount / processedDocumentCount);
        }

        return percent;
    }

}
