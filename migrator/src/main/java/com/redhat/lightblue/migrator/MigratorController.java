package com.redhat.lightblue.migrator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.request.data.DataFindRequest;

public class MigratorController extends AbstractController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigratorController.class);

    private final Random rnd = new Random();

    public static final int JOB_FETCH_BATCH_SIZE = 64;

    private static final class LockRecord {
        final MigrationJob mj;
        final ActiveExecution ae;

        public LockRecord(MigrationJob mj, ActiveExecution ae) {
            this.mj = mj;
            this.ae = ae;
        }
    }

    public MigratorController(Controller controller, MigrationConfiguration migrationConfiguration) {
        super(controller, migrationConfiguration, "Migrators:" + migrationConfiguration.getConfigurationName());
        setName("MigratorController-" + migrationConfiguration.getConfigurationName());
    }

    private LockRecord lock(MigrationJob mj)
            throws Exception {
        ActiveExecution ae = lock(mj.get_id());
        if (ae != null) {
            return new LockRecord(mj, ae);
        } else {
            return null;
        }
    }

    /**
     * Retrieves jobs that are available, and their scheduled time has passed.
     * Returns at most batchSize jobs starting at startIndex
     */
    public MigrationJob[] retrieveJobs(int batchSize, int startIndex, JobType jobType)
            throws IOException, LightblueException {
        LOGGER.debug("Retrieving jobs: batchSize={}, startIndex={}", batchSize, startIndex);

        DataFindRequest findRequest = new DataFindRequest("migrationJob", null);

        List<Query> conditions = new ArrayList<>(Arrays.asList(new Query[] {
                // get jobs for this configuration
                Query.withValue("configurationName", Query.eq, migrationConfiguration.getConfigurationName()),
                // get jobs whose state ara available
                Query.withValue("status", Query.eq, "available"),
                // only get jobs that are
                Query.withValue("scheduledDate", Query.lte, new Date())
        }));

        if (jobType == JobType.GENERATED) {
            LOGGER.debug("Looking for generated job");
            conditions.add(Query.withValue("generated", Query.eq, true));
        } else if (jobType == JobType.NONGENERATED) {
            LOGGER.debug("Looking for non generated job");
            conditions.add(Query.withValue("generated", Query.eq, false));
        }

        findRequest.where(Query.and(conditions));

        findRequest.select(Projection.includeField("*"));

        findRequest.range(startIndex, startIndex + batchSize - 1);

        LOGGER.debug("Finding Jobs to execute: {}", findRequest.getBody());

        return lbClient.data(findRequest, MigrationJob[].class);
    }

    private LockRecord findAndLockMigrationJob()
            throws Exception {
        // We retrieve a batch of migration jobs, and try to lock
        // one of them randomly. This works, because all the jobs
        // we retrieve are already passed their scheduled times,
        // so it doesn't matter in what order they execute. If we
        // can't lock any of the jobs in a given batch, we
        // retrieve the next batch, and try there. Randomness is
        // to prevent flooding: multiple threads starting at the
        // same time should not try to lock resources in the same
        // order, because one will succeed, and all others will
        // fail, and they all will try the next entity in line.
        int startIndex = 0;
        boolean more;
        try {
            do {
                more = true;
                MigrationJob[] jobs = retrieveJobs(JOB_FETCH_BATCH_SIZE, startIndex, getJobTypeToProcess());

                if (jobs == null || jobs.length == 0) {
                    // didn't find the job kind we were looking for, so fetch any
                    jobs = retrieveJobs(JOB_FETCH_BATCH_SIZE, startIndex, JobType.ANY);
                }

                if (jobs != null && jobs.length > 0) {
                    if (jobs.length < JOB_FETCH_BATCH_SIZE) {
                        more = false;
                    }

                    List<MigrationJob> jobList = new LinkedList<>();
                    for (MigrationJob x : jobs) {
                        jobList.add(x);
                    }

                    do {
                        // Pick a job at random
                        int jobIndex = rnd.nextInt(jobList.size());
                        MigrationJob job = jobList.get(jobIndex);
                        // Try to lock it
                        LockRecord lck;
                        if ((lck = lock(job)) != null) {
                            // Locked. Return it
                            return lck;
                        } else {
                            // Can't lock it. Remove from job list
                            jobList.remove(jobIndex);
                        }
                    } while (!jobList.isEmpty() && !isInterrupted());

                } else {
                    more = false;
                }
            } while (more && !isInterrupted());
        } catch (Exception e) {
            LOGGER.error("Exception in findAndLockMigrationJob:" + e, e);
            throw e;
        }
        // No jobs to process
        return null;
    }

    @Override
    public void run() {
        LOGGER.debug("Starting controller thread");
        // This thread never stops
        Breakpoint.checkpoint("MigratorController:start");
        ThreadMonitor monitor=controller.getThreadMonitor();
        while(!stopped) {
            LOGGER.debug("Controller thread for {} is alive",migrationConfiguration.getConfigurationName());
            if(!stopped) {
                // All active threads will notify on migratorThreads when they finish
                synchronized (migratorThreads) {
                    int k = 0;
                    // Are we already running all the threads we can?
                    // Don't include abandoned threads in this count
                    int nThreads=monitor.getThreadCount(migratorThreads,
                                                        ThreadMonitor.Status.alive,
                                                        ThreadMonitor.Status.killed);
                    LOGGER.debug("There are {} active threads for {}",nThreads,migrationConfiguration.getConfigurationName());
                    while(!stopped&&nThreads>=migrationConfiguration.getThreadCount()) {
                        // Wait until someone terminates (1 sec)
                        try {
                            migratorThreads.wait(1000);
                        } catch(InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        if (k++ % 10 == 0) {
                            // refresh configuration every 10 iteration
                            try {
                                MigrationConfiguration x = reloadMigrationConfiguration();
                                if (x == null) {
                                    // Terminate
                                    LOGGER.debug("Controller {} terminating",migrationConfiguration.getConfigurationName());
                                    stopped=true;
                                } else {
                                    migrationConfiguration = x;
                                }
                            } catch (Exception e) {
                                LOGGER.error("Cannot refresh configuration", e);
                            }
                        }
                        nThreads = monitor.getThreadCount(migratorThreads,
                                ThreadMonitor.Status.alive,
                                ThreadMonitor.Status.killed);
                        LOGGER.debug("There are {} active threads for {}", nThreads, migrationConfiguration.getConfigurationName());
                    }
                }
            }
            if(!stopped) {
                LOGGER.debug("Find a migration job to process for {}",migrationConfiguration.getConfigurationName());
                try {
                    Breakpoint.checkpoint("MigratorController:findandlock");
                    LockRecord lockedJob = findAndLockMigrationJob();
                    if (lockedJob != null) {
                        LOGGER.debug("Found migration job {} for {}", lockedJob.mj.get_id(), migrationConfiguration.getConfigurationName());
                        Breakpoint.checkpoint("MigratorController:process");
                        Migrator m = createMigrator(lockedJob.mj, lockedJob.ae);
                        m.registerThreadMonitor(monitor);
                        m.start();
                    } else {
                        if (migrationConfiguration.isSleepIfNoJobs()) {
                            // No jobs are available, wait a bit (10sec-30sec), and retry
                            LOGGER.debug("Waiting for {}", migrationConfiguration.getConfigurationName());
                            Thread.sleep(rnd.nextInt(20000) + 10000);
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOGGER.error("Cannot lock migration job:" + e, e);
                }
            }
        }
        migratorThreads.interrupt();
        Breakpoint.checkpoint("MigratorController:end");
        LOGGER.debug("Ending controller thread for {}", migrationConfiguration.getConfigurationName());
    }

    /**
     * Draws JobType basing on weights defined in configuration. This is to ensure that generated (consistency checker) and
     * non generated (migrator) jobs run in requested proportions.
     *
     * @param cfg
     * @return
     */
    private JobType getJobTypeToProcess() {
        double denominator = migrationConfiguration.getConsistencyCheckerWeight() + migrationConfiguration.getMigratorWeight();

        if (random.nextDouble() <= migrationConfiguration.getConsistencyCheckerWeight() / denominator) {
            return JobType.GENERATED;
        } else {
            return JobType.NONGENERATED;
        }
    }
}
