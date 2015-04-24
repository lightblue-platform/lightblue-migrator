package com.redhat.lightblue.migrator.consistency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.request.AbstractLightblueMetadataRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class ConsistencyCheckerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckerTest.class);

    private final String checkerName = "testChecker";
    private final String hostname = "http://lightblue.io";
    private final String configPath = "lightblue-client.properties";
    private final String sourceConfigPath = "source-lightblue-client.properties";
    private final String destinationConfigPath = "destination-lightblue-client.properties";

    ConsistencyChecker checker;

    LightblueClient client;

    @Before
    public void setUp() throws Exception {
        checker = new ConsistencyChecker();
        checker.setConsistencyCheckerName(checkerName);
        checker.setHostName(hostname);
        checker.setConfigPath(configPath);
        checker.setSourceConfigPath(sourceConfigPath);
        checker.setDestinationConfigPath(destinationConfigPath);
        client = new LightblueHttpClient();
        checker.setClient(client);
    }

    @Test
    public void testGetConsistencyCheckerName() {
        Assert.assertEquals(checkerName, checker.getConsistencyCheckerName());
    }

    @Test
    public void testSetConsistencyCheckerName() {
        checker.setConsistencyCheckerName(hostname);
        Assert.assertEquals(hostname, checker.getConsistencyCheckerName());
    }

    @Test
    public void testGetHostName() {
        Assert.assertEquals(hostname, checker.getHostName());
    }

    @Test
    public void testSetHostName() {
        checker.setHostName(checkerName);
        Assert.assertEquals(checkerName, checker.getHostName());
    }

    @Test
    public void testGetServiceURI() {
        Assert.assertEquals(configPath, checker.getConfigPath());
    }

    @Test
    public void testSetServiceURI() {
        checker.setConfigPath(checkerName);
        Assert.assertEquals(checkerName, checker.getConfigPath());
    }

    @Test
    public void testGetConfigPath() {
        Assert.assertEquals(configPath, checker.getConfigPath());
    }

    @Test
    public void testSetConfigPath() {
        checker.setConfigPath(sourceConfigPath);
        Assert.assertEquals(sourceConfigPath, checker.getConfigPath());
    }

    @Test
    public void testSourceGetConfigPath() {
        Assert.assertEquals(sourceConfigPath, checker.getSourceConfigPath());
    }

    @Test
    public void testSetSourceConfigPath() {
        checker.setSourceConfigPath(destinationConfigPath);
        Assert.assertEquals(destinationConfigPath, checker.getSourceConfigPath());
    }

    @Test
    public void testDestinationGetConfigPath() {
        Assert.assertEquals(destinationConfigPath, checker.getDestinationConfigPath());
    }

    @Test
    public void testSetDesitnationConfigPath() {
        checker.setDestinationConfigPath(sourceConfigPath);
        Assert.assertEquals(sourceConfigPath, checker.getDestinationConfigPath());
    }

    @Test
    public void testExecute() throws Exception {
        ConsistencyChecker checker = new ConsistencyChecker() {
            public int numRuns = 0;

            @Override
            protected List<MigrationJob> getMigrationJobs(MigrationConfiguration configuration) {
                ArrayList<MigrationJob> jobs = new ArrayList<>();
                if (numRuns == 0 || numRuns == 2) {
                    for (int i = 0; i < 10; i++) {
                        MigrationJob job = new MigrationJob() {
                            @Override
                            public void run() {
                                ConsistencyCheckerTest.LOGGER.info("MigrationJob started");
                                ConsistencyCheckerTest.LOGGER.info("MigrationJob completed");
                            }
                        };
                        jobs.add(job);
                    }
                }
                return jobs;
            }

            @Override
            protected List<MigrationConfiguration> getJobConfigurations() {
                ArrayList<MigrationConfiguration> configurations = new ArrayList<>();

                for (int i = 0; i < 5; i++) {
                    MigrationConfiguration config = new MigrationConfiguration();
                    config.setThreadCount(5);
                    configurations.add(config);
                }

                numRuns++;
                if (numRuns > 2) {
                    setRun(false);
                }
                return configurations;
            }

            @Override
            protected MigrationJob getNextAvailableJob() {
                MigrationJob job = new MigrationJob();
                job.setWhenAvailableDate(DateUtils.addSeconds(new Date(), 5));
                return job;
            }

        };
        checker.run();

    }

    @Test
    public void isJobExecutable_NoExecutions() {
        MigrationJob job = new MigrationJob();

        Assert.assertTrue(ConsistencyChecker.isJobExecutable(job));
    }

    @Test
    public void isJobExecutable_ExpiredExecution() {
        MigrationJob job = new MigrationJob();
        MigrationJobExecution exec = new MigrationJobExecution();
        job.setExpectedExecutionMilliseconds(30000);
        exec.setActualStartDate(new Date(System.currentTimeMillis() - job.getExpectedExecutionMilliseconds() * 2));
        List<MigrationJobExecution> execs = new ArrayList<>();
        execs.add(exec);
        job.setJobExecutions(execs);

        Assert.assertTrue(ConsistencyChecker.isJobExecutable(job));
    }

    @Test
    public void isJobExecutable_RunningExecution() {
        MigrationJob job = new MigrationJob();
        MigrationJobExecution exec = new MigrationJobExecution();
        job.setExpectedExecutionMilliseconds(30000);
        exec.setActualStartDate(new Date(System.currentTimeMillis() - job.getExpectedExecutionMilliseconds() / 2));
        List<MigrationJobExecution> execs = new ArrayList<>();
        execs.add(exec);
        job.setJobExecutions(execs);

        Assert.assertFalse(ConsistencyChecker.isJobExecutable(job));
    }

    @Test
    public void getNextAvailableJob() {
        final String pid = "jewzaam was here";

        checker.setClient(new LightblueClient() {

            @Override
            public LightblueResponse metadata(AbstractLightblueMetadataRequest lr) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public LightblueResponse data(AbstractLightblueDataRequest lr) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> T data(AbstractLightblueDataRequest lr, Class<T> type) throws IOException {
                MigrationJob[] jobs = new MigrationJob[1];
                jobs[0] = new MigrationJob();
                jobs[0].setPid(pid);
                return (T) jobs;
            }
        });

        MigrationJob job = checker.getNextAvailableJob();

        Assert.assertNotNull(job);
        Assert.assertEquals(pid, job.getPid());
    }
}
