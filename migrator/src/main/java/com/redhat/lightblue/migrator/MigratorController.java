package com.redhat.lightblue.migrator;

import java.util.Random;
import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.HashMap;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Update;
import com.redhat.lightblue.client.Sort;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.response.LightblueException;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.util.ClientConstants;

public class MigratorController extends AbstractController {

    private static final Logger LOGGER=LoggerFactory.getLogger(MigratorController.class);
   
    private final Random rnd=new Random();

    public static final int JOB_FETCH_BATCH_SIZE=64;

    private static final class LockRecord {
        final MigrationJob mj;
        final ActiveExecution ae;

        public LockRecord(MigrationJob mj,ActiveExecution ae) {
            this.mj=mj;
            this.ae=ae;
        }
    }
            
    public MigratorController(Controller controller,MigrationConfiguration migrationConfiguration) {
        super(controller,migrationConfiguration,"Migrators:"+migrationConfiguration.getConfigurationName());
    }

    private LockRecord lock(MigrationJob mj)
        throws Exception {
        ActiveExecution ae=lock(mj.get_id());
        if(ae!=null)
            return new LockRecord(mj,ae);
        else
            return null;
    }

    /**
     * Retrieves jobs that are available, and their scheduled time has passed. Returns at most batchSize jobs starting at startIndex
     */
    public MigrationJob[] retrieveJobs(int batchSize,int startIndex)
        throws IOException, LightblueException {
        LOGGER.debug("Retrieving jobs: batchSize={}, startIndex={}",batchSize,startIndex);
        
        DataFindRequest findRequest = new DataFindRequest("migrationJob",null);
        
        findRequest.where(
                                    
                          Query.and(
                              // get jobs for this configuration
                                    Query.withValue("configurationName",Query.eq,migrationConfiguration.getConfigurationName()),
                                    // get jobs whose state ara available
                                    Query.withValue("status",Query.eq,"available"),
                                    // only get jobs that are 
                                    Query.withValue("scheduledDate",Query.lte,new Date())
                                    )
                          );
        findRequest.select(Projection.includeField("*"));
        
        // sort by scheduledDate ascending to process oldest jobs first
        findRequest.sort(Sort.asc("scheduledDate"));
        
        findRequest.range(startIndex, startIndex+batchSize-1);
        
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
        int startIndex=0;
        boolean more;
        try {
            do {
                more=true;
                MigrationJob[] jobs=retrieveJobs(JOB_FETCH_BATCH_SIZE,startIndex);
                if(jobs!=null&&jobs.length>0) {
                    if(jobs.length<JOB_FETCH_BATCH_SIZE)
                        more=false;
                    
                    List<MigrationJob> jobList=new LinkedList<>();
                    for(MigrationJob x:jobs)
                        jobList.add(x);
                    
                    do {
                        // Pick a job at random
                        int jobIndex=rnd.nextInt(jobList.size());
                        MigrationJob job=jobList.get(jobIndex);
                        // Try to lock it
                        LockRecord lck;
                        if((lck=lock(job))!=null) {
                            // Locked. Return it
                            return lck;
                        } else {
                            // Can't lock it. Remove from job list
                            jobList.remove(jobIndex);
                        }
                    } while(!jobList.isEmpty()&&!isInterrupted());
                    
                } else
                    more=false;
            } while(more&&!isInterrupted());
        } catch (Exception e) {
            LOGGER.error("Exception in findAndLockMigrationJob:"+e,e);
            throw e;
        }
        // No jobs to process
        return null;
    }

    @Override
    public void run() {
        LOGGER.debug("Starting controller thread");
        boolean interrupted=false;
        // This thread never stops
        Breakpoint.checkpoint("MigratorController:start");
        while(!interrupted) {

            interrupted=isInterrupted();
            if(!interrupted) {
                // All active threads will notify on migratorThreads when they finish
                synchronized(migratorThreads) {
                    int k=0;
                    // Are we already running all the threads we can?
                    while(!interrupted&&migratorThreads.activeCount()>=migrationConfiguration.getThreadCount()) {
                        // Wait until someone terminates (1 sec)
                        try {
                            migratorThreads.wait(1000);
                        } catch(InterruptedException e) {
                            interrupted=true;
                        }
                        if(k++%10==0) {
                            // refresh configuration every 10 iteration
                            MigrationConfiguration x=reloadMigrationConfiguration();
                            if(x==null) {
                                // Terminate
                                LOGGER.debug("Controller terminating");
                                interrupted=true;
                            } else {
                                migrationConfiguration=x;
                            }
                        }
                    }
                }
            }
            if(!interrupted) {
                LOGGER.debug("Find a migration job to process");
                try {
                    Breakpoint.checkpoint("MigratorController:findandlock");
                    LockRecord lockedJob=findAndLockMigrationJob();
                    if(lockedJob!=null) {
                        LOGGER.debug("Found migration job {}",lockedJob.mj.get_id());
                        Breakpoint.checkpoint("MigratorController:process");
                        createMigrator(lockedJob.mj,lockedJob.ae).start();
                    } else {
                        // No jobs are available, wait a bit (10sec-30sec), and retry
                        LOGGER.debug("Waiting");
                        Thread.sleep(rnd.nextInt(20000)+10000);
                    }
                } catch (InterruptedException ie) {
                    interrupted=true;
                } catch (Exception e) {
                    LOGGER.error("Cannot lock migration job:"+e,e);
                }
            }
        }
        migratorThreads.interrupt();
        Breakpoint.checkpoint("MigratorController:end");
        LOGGER.debug("Ending controller thread");
    }
}
