package com.redhat.lightblue.migrator.monitor.NMP;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.migrator.MigrationConfiguration;
import com.redhat.lightblue.migrator.MigrationJob;
import com.redhat.lightblue.migrator.monitor.JobType;
import com.redhat.lightblue.migrator.monitor.Monitor;
import com.redhat.lightblue.migrator.monitor.MonitorConfiguration;
import com.redhat.lightblue.migrator.monitor.Notifier;

/** Monitor implementation for {@link JobType#NEW_MIGRATION_PERIODS} */
public class NMPMonitor extends Monitor {

    public NMPMonitor(MonitorConfiguration monitorCfg) {
        super(monitorCfg);
    }

    @Override
    public void runCheck(final Notifier... notifiers) throws LightblueException {
        int periods = (monitorCfg.getPeriods() == null) ? 1 : monitorCfg.getPeriods();

        List<String> configurationsMissingJobs = new ArrayList<>();
        for (MigrationConfiguration cfg : findMigrationConfigurations()) {
            long period = periods * parsePeriod(cfg.getPeriod());
            Date endDate = new Date();
            Date startDate = new Date(endDate.getTime() - period);

            int jobs = countMigrationJobs(cfg.getConfigurationName(), startDate, endDate);
            if (jobs <= 0) {
                configurationsMissingJobs.add(cfg.getConfigurationName());
            }
        }

        if (configurationsMissingJobs.isEmpty()) {
            for (Notifier n : notifiers) {
                n.sendSuccess();
            }
        } else {
            String message = "Jobs not being created: " + StringUtils.join(configurationsMissingJobs, ",");
            for (Notifier n : notifiers) {
                n.sendFailure(message);
            }
        }
    }

    /**
     * Returns the period in msecs
     */
    private static long parsePeriod(String periodStr) {
        PeriodFormatter fmt = PeriodFormat.getDefault();
        Period p = fmt.parsePeriod(periodStr);
        return p.toStandardDuration().getMillis();
    }

    private MigrationConfiguration[] findMigrationConfigurations() throws LightblueException {
        DataFindRequest findConfigurations = new DataFindRequest(MigrationConfiguration.ENTITY_NAME);
        findConfigurations.where(Query.withValue("period", Query.neq,
                Literal.pojo(null)));
        findConfigurations.select(
                Projection.includeField("configurationName"),
                Projection.includeField("period"));

        return lightblueClient.data(findConfigurations, MigrationConfiguration[].class);
    }

    private int countMigrationJobs(String configurationName, Date startDate, Date endDate) throws LightblueException {
        DataFindRequest findJobs = new DataFindRequest(MigrationJob.ENTITY_NAME);
        findJobs.where(
            Query.and(
                Query.withValue("configurationName", Query.eq, configurationName),
                Query.withValue("scheduledDate", Query.gte, startDate),
                Query.withValue("scheduledDate", Query.lte, endDate)
            )
        );
        findJobs.select(new Projection[]{Projection.excludeFieldRecursively("*")}, 0, 0);

        LightblueDataResponse response = lightblueClient.data(findJobs);
        return response.parseMatchCount();
    }

}
