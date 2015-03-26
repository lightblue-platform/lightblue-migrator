package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.client.expression.query.ArrayQuery.withSubfield;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.or;
import static com.redhat.lightblue.client.expression.query.UnaryLogicalQuery.not;
import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.util.ClientConstants;

public class ConsistencyChecker implements Runnable {

    LightblueClient client;

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyChecker.class);

    public static final int MAX_JOB_WAIT_MSEC = 30 * 60 * 1000; // 30 minutes
    public static final int MAX_THREAD_WAIT_MSEC = 5 * 60 * 1000; // 5 minutes

    private String consistencyCheckerName;
    private String hostName;
    private String configPath;
    private String migrationConfigurationEntityVersion;
    private String migrationJobEntityVersion;
    private String sourceConfigPath;
    private String destinationConfigPath;

    private boolean run = true;

    public void setRun(boolean run) {
        this.run = run;
    }

    public String getConsistencyCheckerName() {
        return consistencyCheckerName;
    }

    public void setConsistencyCheckerName(String consistencyCheckerName) {
        this.consistencyCheckerName = consistencyCheckerName;
    }

    public LightblueClient getClient() {
        return client;
    }

    public void setClient(LightblueClient client) {
        this.client = client;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getMigrationConfigurationEntityVersion() {
        return migrationConfigurationEntityVersion;
    }

    public void setMigrationConfigurationEntityVersion(String migrationConfigurationEntityVersion) {
        this.migrationConfigurationEntityVersion = migrationConfigurationEntityVersion;
    }

    public String getMigrationJobEntityVersion() {
        return migrationJobEntityVersion;
    }

    public void setMigrationJobEntityVersion(String migrationJobEntityVersion) {
        this.migrationJobEntityVersion = migrationJobEntityVersion;
    }

    public String getSourceConfigPath() {
        return sourceConfigPath;
    }

    public void setSourceConfigPath(String configPath) {
        this.sourceConfigPath = configPath;
    }

    public String getDestinationConfigPath() {
        return destinationConfigPath;
    }

    public void setDestinationConfigPath(String configPath) {
        this.destinationConfigPath = configPath;
    }

    @Override
    public void run() {

        LOGGER.info("From CLI - consistencyCheckerName: " + getConsistencyCheckerName() + " hostName: " + getHostName());

        LightblueHttpClient httpClient;
        if (configPath != null) {
            httpClient = new LightblueHttpClient(configPath);
        } else {
            httpClient = new LightblueHttpClient();
        }
        client = new LightblueHystrixClient(httpClient, "migrator", "primaryClient");

        while (run && !Thread.interrupted()) {
            List<ExecutorService> executors = new ArrayList<>();
            List<MigrationConfiguration> configurations = getJobConfigurations();

            for (MigrationConfiguration configuration : configurations) {
                if (configuration.getThreadCount() < 1) {
                    continue;
                }

                configuration.setConfigFilePath(configPath);
                configuration.setMigrationJobEntityVersion(migrationJobEntityVersion);
                List<MigrationJob> jobs = getMigrationJobs(configuration);

                if (!jobs.isEmpty()) {
                    ExecutorService jobExecutor = Executors.newFixedThreadPool(configuration.getThreadCount());
                    executors.add(jobExecutor);
                    for (MigrationJob job : jobs) {
                        job.setJobConfiguration(configuration);
                        job.setOwner(getConsistencyCheckerName());
                        job.setHostName(getHostName());
                        job.setPid(ManagementFactory.getRuntimeMXBean().getName());
                        job.setSourceConfigPath(sourceConfigPath);
                        job.setDestinationConfigPath(destinationConfigPath);
                        jobExecutor.execute(job);
                    }
                }
            }
            
            LOGGER.info("Done setting up job executors");

            if (executors.isEmpty()) {
                if (run && !Thread.interrupted()) {
                    MigrationJob nextJob = getNextAvailableJob();
                    long timeUntilNextJob = MAX_JOB_WAIT_MSEC;
                    if (nextJob != null && nextJob.getWhenAvailableDate() != null) {
                        timeUntilNextJob = nextJob.getWhenAvailableDate().getTime() - new Date().getTime();
                    }
                    if (timeUntilNextJob > MAX_JOB_WAIT_MSEC) {
                        timeUntilNextJob = MAX_JOB_WAIT_MSEC;
                        LOGGER.info("Time to next job ({} msec) is greater than max wait time ({} msec}. Defaulting to max.", timeUntilNextJob, MAX_JOB_WAIT_MSEC);
                    }
                    try {
                        LOGGER.info("Waiting for next job, sleeping for {} msec", timeUntilNextJob);
                        Thread.sleep(timeUntilNextJob);
                    } catch (InterruptedException e) {
                        run = false;
                    }
                }
            } else {
                for (ExecutorService executor : executors) {
                    executor.shutdown();
                }
                if ((!Thread.interrupted())) {
                    try {
                        for (ExecutorService executor : executors) {
                            executor.awaitTermination(MAX_THREAD_WAIT_MSEC, TimeUnit.MILLISECONDS);
                        }
                    } catch (InterruptedException e) {
                        run = false;
                    }
                }
            }
            
            LOGGER.info("Job executors done");
        }
    }

    protected List<MigrationJob> getMigrationJobs(MigrationConfiguration configuration) {
        LOGGER.info("Loading jobs for {}", configuration.getConfigurationName());
        List<MigrationJob> jobs = new ArrayList<>();
        try {
            DataFindRequest findRequest = new DataFindRequest("migrationJob", migrationJobEntityVersion);
            /*
                and:
                    whenAvailableDate <= new Date()
                    or:
                        jobExecutions# = 0
                        not:
                            jobExecutions.jobStatus $in ['COMPLETED_SUCCESS', 'COMPLETED_PARTIAL']
                        and:
                            not:
                                jobExecutions.jobStatus $not_in [COMPLETED_SUCCESS', 'COMPLETED_PARTIAL']
                            jobExecutions.actualStartDate < (new Date() - expectedExecutionMilliseconds)

             */
            findRequest.where(
                and(
                    withValue("whenAvailableDate <= " + ClientConstants.getDateFormat().format(new Date())),
                    or(
                        not(

                            withSubfield("jobExecutions", withValue("jobStatus  $in ['COMPLETED_SUCCESS', 'COMPLETED_PARTIAL']"))
                        ),
                        and(
                            not(

                                    withSubfield("jobExecutions", withValue("jobStatus  $in ['COMPLETED_SUCCESS', 'COMPLETED_PARTIAL']"))
                            ),
                            // TODO check how lightblue will handle the next comparison (if it can handle this case), suggestions?
                            withValue("whenAvailableDate + expectedExecutionMilliseconds < " + ClientConstants.getDateFormat().format(new Date()))
                        )
                    )
                )
            );
            findRequest.select(includeFieldRecursively("*"));

            LOGGER.debug("Finding Jobs to execute: {}", findRequest.getBody());
            jobs.addAll(Arrays.asList(client.data(findRequest, MigrationJob[].class)));
            LOGGER.info("Loaded jobs for {}: {}", configuration.getConfigurationName(), jobs.size());
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationJobs", e);
        }
        return jobs;
    }

    protected List<MigrationConfiguration> getJobConfigurations() {
        List<MigrationConfiguration> configurations = new ArrayList<>();
        try {
            DataFindRequest findRequest = new DataFindRequest("migrationConfiguration", migrationConfigurationEntityVersion);
            findRequest.where(withValue("consistencyCheckerName = " + getConsistencyCheckerName()));
            findRequest.select(includeFieldRecursively("*"));

            LOGGER.debug("Finding Job Configurations: {}", findRequest.getBody());
            configurations.addAll(Arrays.asList(client.data(findRequest, MigrationConfiguration[].class)));
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationConfigurations", e);
        }
        return configurations;
    }

    /**
     * Gets the next job available for processing.
     * @return 
     */
    protected MigrationJob getNextAvailableJob() {
        MigrationJob job = null;
        try {
            DataFindRequest findRequest = new DataFindRequest("migrationJob", migrationJobEntityVersion);
            findRequest.where(withValue("whenAvailableDate >= " + ClientConstants.getDateFormat().format(new Date())));
            findRequest.sort(new SortCondition("whenAvailableDate", SortDirection.ASC));
            findRequest.range(0, 0); // range is inclusive
            findRequest.select(includeFieldRecursively("*"));

            LOGGER.debug("Get next job: {}", findRequest.getBody());
            job = client.data(findRequest, MigrationJob.class);
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationJob", e);
        }
        return job;
    }

}
