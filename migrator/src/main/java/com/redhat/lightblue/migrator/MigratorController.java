package com.redhat.lightblue.migrator;

import java.util.Random;
import java.util.Date;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.ExpressionOperation;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.expression.update.SetUpdate;
import com.redhat.lightblue.client.expression.update.PathValuePair;
import com.redhat.lightblue.client.expression.update.AppendUpdate;
import com.redhat.lightblue.client.expression.update.LiteralRValue;
import com.redhat.lightblue.client.expression.update.ObjectRValue;
import com.redhat.lightblue.client.expression.update.ForeachUpdate;
import com.redhat.lightblue.client.util.ClientConstants;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;
import static com.redhat.lightblue.client.projection.FieldProjection.includeField;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;

public class MigratorController extends Thread {

    private static final Logger LOGGER=LoggerFactory.getLogger(MigratorController.class);
    
    private MigrationConfiguration migrationConfiguration;
    private final Class migratorClass;
    private final Controller controller;
    private final LightblueClient lbClient;
    private final Random rnd=new Random();
    private final ThreadGroup migratorThreads;

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
        this.migrationConfiguration=migrationConfiguration;
        this.controller=controller;
        lbClient=controller.getLightblueClient();

        if(migrationConfiguration.getMigratorClass()==null)
            migratorClass=DefaultMigrator.class;
        else
            try {
                migratorClass=Class.forName(migrationConfiguration.getMigratorClass());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        
        migratorThreads=new ThreadGroup("Migrators:"+migrationConfiguration.getConfigurationName());
    }

    public ThreadGroup getMigratorThreads() {
        return migratorThreads;
    }

    public Controller getController() {
        return controller;
    }

    public MigrationConfiguration getMigrationConfiguration() {
        return migrationConfiguration;
    }

    public MigrationConfiguration reloadMigrationConfiguration() {
        try {
            LOGGER.debug("Reloading migration configuration {}",migrationConfiguration.get_id());
            DataFindRequest findRequest = new DataFindRequest("migrationConfiguration",null);
            findRequest.where(withValue("_id",ExpressionOperation.EQ,migrationConfiguration.get_id()));
            findRequest.select(includeFieldRecursively("*"));
            LOGGER.debug("Loading configuration");
            return lbClient.data(findRequest, MigrationConfiguration.class);
        } catch (Exception e) {
            LOGGER.error("Cannot reload migration configuration:"+e);
        }
        return null;
    }
    
    /**
     * Retrieves jobs that are available, and their scheduled time has passed. Returns at most batchSize jobs starting at startIndex
     */
    public MigrationJob[] retrieveJobs(int batchSize,int startIndex)
        throws IOException {
        LOGGER.debug("Retrieving jobs: batchSize={}, startIndex={}",batchSize,startIndex);
        
        DataFindRequest findRequest = new DataFindRequest("migrationJob",null);
        
        findRequest.where(
                          and(
                              // get jobs for this configuration
                              withValue("configurationName",ExpressionOperation.EQ,migrationConfiguration.getConfigurationName()),
                              // get jobs whose state ara available
                              withValue("status",ExpressionOperation.EQ,"available"),
                              // only get jobs that are 
                              withValue("scheduledDate",ExpressionOperation.LTE,ClientConstants.getDateFormat().format(new Date()))
                              )
                          );
        findRequest.select(includeField("*"));
        
        // sort by scheduledDate ascending to process oldest jobs first
        findRequest.sort(new SortCondition("scheduledDate", SortDirection.ASCENDING));
        
        findRequest.range(startIndex, startIndex+batchSize-1);
        
        LOGGER.debug("Finding Jobs to execute: {}", findRequest.getBody());
        
        return lbClient.data(findRequest, MigrationJob[].class);
    }

    /**
     * Attempts to lock a migration job. If successful, return the migration job and the active execution
     */
    private LockRecord lock(MigrationJob mj)
        throws Exception {
        DataInsertRequest insRequest=new DataInsertRequest("activeExecution",null);
        ActiveExecution ae=new ActiveExecution();
        ae.setMigrationJobId(mj.get_id());
        ae.setStartTime(new Date());
        
        insRequest.create(ae);
        insRequest.returns(includeFieldRecursively("*"));
        LightblueResponse rsp;
        try {
            rsp=lbClient.data(insRequest);
            if(rsp.hasError())
                return null;
        } catch (Exception e) {
            return null;
        }
        if(rsp.parseModifiedCount()==1) {
            return new LockRecord(mj,rsp.parseProcessed(ActiveExecution.class));
        } else
            return null;
    }

    private void unlock(String id) {
        DataDeleteRequest req=new DataDeleteRequest("activeExecution",null);
        req.where(withValue("_id",ExpressionOperation.EQ,id));
        try {
            LightblueResponse rsp=lbClient.data(req);
        } catch(Exception e) {
            LOGGER.error("Cannot delete lock {}",id);
        }
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
        do {
            more=true;
            MigrationJob[] jobs=retrieveJobs(JOB_FETCH_BATCH_SIZE,startIndex);
            if(jobs!=null&&jobs.length>0) {
                if(jobs.length<JOB_FETCH_BATCH_SIZE)
                    more=false;

                List<MigrationJob> jobList=(List<MigrationJob>)Arrays.asList(jobs);

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
                } while(!jobList.isEmpty());
                
            } else
                more=false;
        } while(more);
        // No jobs to process
        return null;
    }

    private void processMigrationJob(LockRecord lck)
        throws Exception {
        Migrator migrator=(Migrator)migratorClass.getConstructor(ThreadGroup.class).newInstance(migratorThreads);
        migrator.setController(this);
        migrator.setMigrationJob(lck.mj);
        migrator.setActiveExecution(lck.ae);
        migrator.start();
    }
            
    @Override
    public void run() {
        LOGGER.debug("Starting controller thread");
        boolean interrupted=false;
        // This thread never stops
        Breakpoint.checkpoint("MigratorController:start");
        while(!interrupted) {
            interrupted=Thread.isInterrupted();
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
                        processMigrationJob(lockedJob);
                        // Unlock
                        unlock(lockedJob.ae.get_id());
                        Breakpoint.checkpoint("MigratorController:unlock");
                    } else {
                        // No jobs are available, wait a bit (10sec-30sec), and retry
                        LOGGER.debug("Waiting");
                        Thread.sleep(rnd.nextInt(20000)+10000);
                    }
                } catch (InterruptedException ie) {
                    interrupted=true;
                } catch (Exception e) {
                    LOGGER.error("Cannot lock migration job:"+e);
                }
            }
        }
        Breakpoint.checkpoint("MigratorController:end");
        LOGGER.debug("Ending controller thread");
    }
}
