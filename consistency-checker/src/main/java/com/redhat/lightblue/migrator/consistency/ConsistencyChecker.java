package com.redhat.lightblue.migrator.consistency;

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
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.util.ClientConstants;

public class ConsistencyChecker implements Runnable{

    LightblueClient client;

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyChecker.class);

    public static final int MAX_JOB_WAIT_TIME = 86400000; // 24 hours
    public static final int MAX_THREAD_WAIT_TIME = 21600; // 6 hours
    public static final int DEFAULT_WAIT = 30000;         // 30 minutes

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

        if (configPath != null) {
            client = new LightblueHttpClient(configPath);
        } else {
            client = new LightblueHttpClient();
        }

        while (run && !Thread.interrupted()) {
            List<ExecutorService> executors = new ArrayList<>();
            List<MigrationConfiguration> configurations = getJobConfigurations();

            for (MigrationConfiguration configuration : configurations) {
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

            if (executors.isEmpty()) {
                if(run && !Thread.interrupted()){
                    MigrationJob nextJob = getNextAvailableJob();
                    long timeUntilNextJob = DEFAULT_WAIT;
                    if(nextJob != null){
                        timeUntilNextJob = nextJob.getWhenAvailableDate().getTime() - new Date().getTime();
                    }
                    try{
                        Thread.sleep((timeUntilNextJob > MAX_JOB_WAIT_TIME) ? MAX_JOB_WAIT_TIME : timeUntilNextJob);
                    }
                    catch (InterruptedException e){
                        run = false;
                    }
                }
            } else {
                for (ExecutorService executor : executors) {
                    executor.shutdown();
                }
                if((!Thread.interrupted())){
                    try{
                        for (ExecutorService executor : executors) {
                            executor.awaitTermination(MAX_THREAD_WAIT_TIME, TimeUnit.MILLISECONDS);
                        }
                    }
                    catch (InterruptedException e) {
                        run = false;
                    }
                }
            }
        }
    }

    protected List<MigrationJob> getMigrationJobs(MigrationConfiguration configuration) {
        List<MigrationJob> jobs = new ArrayList<MigrationJob>();
        try {
            DataFindRequest findRequest = new DataFindRequest("migrationJob", migrationJobEntityVersion);
            findRequest.where(withValue("configurationName = " + configuration.getConfigurationName()));
            findRequest.select(includeFieldRecursively("*"));
            jobs.addAll(Arrays.asList(client.data(findRequest, MigrationJob[].class)));
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationJobs", e);
        }
        return jobs;
    }

    protected List<MigrationConfiguration> getJobConfigurations() {
        List<MigrationConfiguration> configurations = new ArrayList<MigrationConfiguration>();
        try {
            DataFindRequest findRequest = new DataFindRequest("migrationConfiguration", migrationConfigurationEntityVersion);
            findRequest.where(withValue("consistencyCheckerName = " + getConsistencyCheckerName()));
            findRequest.select(includeFieldRecursively("*"));
            configurations.addAll(Arrays.asList(client.data(findRequest, MigrationConfiguration[].class)));
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationConfigurations", e);
        }
        return configurations;
    }

    protected MigrationJob getNextAvailableJob() {
        MigrationJob job = null;
        try {
            DataFindRequest findRequest = new DataFindRequest("migrationJob", migrationJobEntityVersion);
            findRequest.where(withValue("whenAvailableDate >= " + ClientConstants.getDateFormat().format(new Date())));
            findRequest.sort(new SortCondition("whenAvailableDate", SortDirection.ASC));
            findRequest.range(0, 1);
            findRequest.select(includeFieldRecursively("*"));
            job = client.data(findRequest, MigrationJob.class);
        } catch (IOException e) {
            LOGGER.error("Problem getting migrationJob", e);
        }
        return job;
    }

}
