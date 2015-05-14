package com.redhat.lightblue.migrator;

import static com.redhat.lightblue.client.expression.query.ArrayQuery.withSubfield;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.util.ClientConstants;
import java.util.GregorianCalendar;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ConsistencyChecker implements Runnable {

    LightblueClient client;

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyChecker.class);

    public static final int MAX_JOB_WAIT_MSEC = 30 * 60 * 1000; // 30 minutes
    public static final int MAX_JOBS_PER_ENTITY = 2000; // # of jobs to pick up each execution
    public static final int MAX_EXECUTOR_TERMINATION_WAIT_MSEC = 30 * 60 * 1000; // 30 minutes

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
            List<MigrationConfiguration> configurations=getJobConfigurations();
            List<Future<?>> futures = new ArrayList<>();

            for (MigrationConfiguration configuration : configurations) {
                if (configuration.getThreadCount() < 1) {
                    continue;
                }

                configuration.setConfigFilePath(configPath);
                configuration.setMigrationJobEntityVersion(migrationJobEntityVersion);
                List<MigrationJob> allJobs = getMigrationJobs(configuration);
                List<MigrationJob> jobs = new ArrayList<>();

                LOGGER.info("Loaded jobs for {}: {}", configuration.getConfigurationName(), allJobs.size());

                // loop through jobs to check if job can be executed
                for (MigrationJob job : allJobs) {
                    if (isJobExecutable(job)) {
                        // must initialize job before marking execution as dead
                        job.setJobConfiguration(configuration);
                        job.setOwner(getConsistencyCheckerName());
                        job.setHostName(getHostName());
                        job.setPid(ManagementFactory.getRuntimeMXBean().getName());
                        job.setSourceConfigPath(sourceConfigPath);
                        job.setDestinationConfigPath(destinationConfigPath);

                        try {
                            // mark old expirations as dead
                            markRunningJobExecutionsAsDead(job);
                        } catch (IOException ex) {
                            LOGGER.warn("Unable to mark job as dead: {}", job.get_id());
                        }
                        // add to list of jobs to process
                        jobs.add(job);
                    }
                }

                if (!jobs.isEmpty()) {
                    LOGGER.info("Executing {} of {} loaded jobs for {}", jobs.size(), allJobs.size(), configuration.getConfigurationName());
                }

                if (!jobs.isEmpty()) {
                    ExecutorService jobExecutor = Executors.newFixedThreadPool(configuration.getThreadCount());
                    executors.add(jobExecutor);
                    for (MigrationJob job : allJobs) {
                        futures.add(jobExecutor.submit(job));
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
                for (Future future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException ex) {
                        run = false;
                        LOGGER.error("Future was interrupted, will stop execution of migrator");
                    } catch (ExecutionException ex) {
                        LOGGER.warn("Future execution failed", ex);
                    }
                }
            }

            LOGGER.info("Job executors done");
        }

        LOGGER.info("ConsistencyChecker done");
    }

    protected List<MigrationConfiguration> getJobConfigurations() {
        List<MigrationConfiguration> configurations;
        try {
            configurations=Arrays.asList(Main.getMigrationConfiguration(client,getMigrationConfigurationEntityVersion(),
                                                                        getConsistencyCheckerName()));
        } catch (Exception e) {
            LOGGER.error("Problem getting migrationConfigurations", e);
            configurations=new ArrayList<>();
        }
        return configurations;
    }
    
    protected List<MigrationJob> getMigrationJobs(MigrationConfiguration configuration) {
        LOGGER.info("Loading jobs for {}", configuration.getConfigurationName());
        List<MigrationJob> jobs = new ArrayList<>();
        try {
            DataFindRequest findRequest = new DataFindRequest("migrationJob", migrationJobEntityVersion);

            findRequest.where(
                    and(
                            // get jobs for this configuration
                            withValue("configurationName = " + configuration.getConfigurationName()),
                            // only get jobs that are available now
                            withValue("whenAvailableDate <= " + ClientConstants.getDateFormat().format(new Date())),
                            // only get jobs where there does NOT exist an execution with a complete status
                            not(
                                    withSubfield("jobExecutions", withValue("jobStatus $in [COMPLETED_SUCCESS, COMPLETED_PARTIAL]"))
                            )
                    )
            );
            findRequest.select(includeFieldRecursively("*"));

            // sort by whenAvailableDate ascending to process oldest jobs first
            findRequest.sort(new SortCondition("whenAvailableDate", SortDirection.ASCENDING));

            // only pick up the first MAX_JOBS_PER_ENTITY jobs
            findRequest.range(0, MAX_JOBS_PER_ENTITY);
            
            LOGGER.debug("Finding Jobs to execute: {}", findRequest.getBody());

            jobs.addAll(Arrays.asList(client.data(findRequest, MigrationJob[].class)));
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationJobs", e);
        }
        return jobs;
    }

    /**
     * Helper method to determine if a job can be processed right now based on
     * the execution data (if present). If no execution data exists, it is
     * automatically executable.
     */
    protected static boolean isJobExecutable(MigrationJob job) {
        boolean executable;
        long now = GregorianCalendar.getInstance().getTimeInMillis();

        if (job.getJobExecutions() != null && !job.getJobExecutions().isEmpty()) {
            // get newest execution start date
            long newestActualStartDate = 0;
            for (MigrationJobExecution exec : job.getJobExecutions()) {
                if (exec.getActualStartDate() != null && newestActualStartDate < exec.getActualStartDate().getTime()) {
                    newestActualStartDate = exec.getActualStartDate().getTime();
                }
            }
            // get the time (msec) at which we call this job too old
            long expirationTime = newestActualStartDate + job.getExpectedExecutionMilliseconds();

            // this job is too old if expirationTime <= now, else it can be processed
            executable = expirationTime <= now;
        } else {
            // hasn't been processed before, we can process it now
            executable = true;
        }

        return executable;
    }

    /**
     * Update all non-complete job executions as "dead".
     *
     * @param job the job to cleanup
     */
    protected static void markRunningJobExecutionsAsDead(MigrationJob job) throws IOException {
        int psn = 0;
        for (MigrationJobExecution exec : job.getJobExecutions()) {
            if (exec.getJobStatus() != null && exec.getJobStatus().isRunning()) {
                LOGGER.info("Marking job {} execution {} as {}", job.get_id(), psn, JobStatus.COMPLETED_DEAD);
                exec.setJobStatus(JobStatus.COMPLETED_DEAD);
                exec.setActualEndDate(new Date());
                job.markExecutionStatusAndEndDate(psn, JobStatus.COMPLETED_DEAD, true);
            }
            psn++;
        }
    }


    /**
     * Gets the next job available for processing.
     *
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
            MigrationJob[] jobs = client.data(findRequest, MigrationJob[].class);
            if (jobs != null && jobs.length > 0) {
                job = jobs[0];
            }
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationJob", e);
        }
        return job;
    }

}
