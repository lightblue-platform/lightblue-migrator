package com.redhat.lightblue.migrator;

import java.util.Random;
import java.util.Date;
import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.util.ClientConstants;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;
import static com.redhat.lightblue.client.projection.FieldProjection.includeField;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;

public class MigratorController implements Runnable {

    private static final Logger LOGGER=LoggerFactory.getLogger(MigratorController.class);
    
    private final MigrationConfiguration migrationConfiguration;
    private final Controller controller;
    private final LightblueClient lbClient;
    private final Random rnd=new Random();

    public static final int JOB_FETCH_BATCH_SIZE=64;

    public MigratorController(MigrationConfiguration migrationConfiguration,
                              Controller controller) {
        this.migrationConfiguration=migrationConfiguration;
        this.controller=controller;
        lbClient=controller.getLightblueClient();
    }

    public MigrationConfiguration getMigrationConfiguration() {
        return migrationConfiguration;
    }

    /**
     * Retrieves jobs that are available, and their scheduled time has passed. Returns at most batchSize jobs starting at startIndex
     */
    public MigrationJob[] retrieveJobs(int batchSize,int startIndex)
        throws IOException {
        LOGGER.debug("Retrieving jobs: batchSize={}, startIndex={}",batchSize,startIndex);
        
        DataFindRequest findRequest = new DataFindRequest("migrationJob",
                                                          migrationConfiguration.getMigrationJobEntityVersion());
        
        findRequest.where(
                          and(
                              // get jobs for this configuration
                              withValue("configurationName = " + migrationConfiguration.getConfigurationName()),
                              // get jobs whose state ara available
                              withValue("status = available"),
                              // only get jobs that are 
                              withValue("scheduledDate <= " + ClientConstants.getDateFormat().format(new Date()))
                              )
                          );
        findRequest.select(includeFieldRecursively("*"));
        
        // sort by whenAvailableDate ascending to process oldest jobs first
        findRequest.sort(new SortCondition("scheduledDate", SortDirection.ASCENDING));
        
        findRequest.range(startIndex, startIndex+batchSize-1);
        
        LOGGER.debug("Finding Jobs to execute: {}", findRequest.getBody());
        
        return lbClient.data(findRequest, MigrationJob[].class);;
    }

    private boolean lock(MigrationJob mj) {
        DataInsertRequest insRequest=new DataInsertRequest("activeExecution",null);
        ActiveExecution ae=new ActiveExecution();
        ae.setMigrationJobId(mj.getMigrationJobId());
        ae.setStartTime(new Date());
        
        insRequest.create(ae);
        insRequest.returns(includeField("_id"));
        LightblueResponse rsp;
        try {
            rsp=lbClient.data(insRequest);
            if(rsp.hasError())
                return false;
        } catch (Exception e) {
            return false;
        }
        return rsp.parseModifiedCount()==1;
    }
    
    private MigrationJob lockMigrationJob() {
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
                    if(lock(job)) {
                        // Locked. Return it
                        return job;
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
    
    @Override
    public void run() {
        LOGGER.debug("Starting controller thread");
        // This thread never stops
        while(!Thread.interrupted()) {

            LOGGER.debug("Find a migration job to process");
            
            
        }
        LOGGER.debug("Ending controller thread");
    }
}
