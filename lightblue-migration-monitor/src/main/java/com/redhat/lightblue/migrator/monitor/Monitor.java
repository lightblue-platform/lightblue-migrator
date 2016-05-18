package com.redhat.lightblue.migrator.monitor;

import java.util.Date;

import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.migrator.MigrationConfiguration;
import com.redhat.lightblue.migrator.MigrationJob;

public class Monitor extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(Monitor.class);

    private final LightblueClient lightblueClient;

    public Monitor(MainConfiguration cfg) {
        if (cfg.getClientConfig() != null) {
            lightblueClient = new LightblueHttpClient(cfg.getClientConfig());
        } else {
            lightblueClient = new LightblueHttpClient();
        }
    }

    @Override
    public void run() {
        LOGGER.debug("Starting migrator monitor");
        boolean stop = false;

        while (!isInterrupted() && !stop) {
            try {
                process();
            } catch (Exception e) {
                LOGGER.error("Error during configuration load:" + e);
            }

            if (!stop) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    stop = true;
                }
            }
        }
    }

    private void process() throws Exception {
        for (MigrationConfiguration cfg : findMigrationConfigurations()) {
            long period = parsePeriod(cfg.getPeriod());
            Date endDate = new Date();
            Date startDate = new Date(endDate.getTime() - period);

            int jobs = countMigrationJobs(cfg.getConfigurationName(), startDate, endDate);
            if (jobs <= 0) {
                //TODO some sort of alert
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
