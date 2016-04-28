package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.*;
import com.redhat.lightblue.client.request.data.*;

public class CleanupThread extends Thread {

    private static final Logger LOGGER=LoggerFactory.getLogger(CleanupThread.class);

    private long period=40l*60l*1000l; // 40 minutes, 4*threadMonitor
    private long oldJobThreshold=1000l*60l*60l*24l*7l; // 7 days

    private final Controller controller;

    public CleanupThread(Controller controller) {
        this.controller=controller;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long l) {
        period=l;
    }

    public long getOldJobThreshold() {
        return oldJobThreshold;
    }

    public void setOldJobThreshold(long l) {
        oldJobThreshold=l;
    }

    private Literal[] ids(MigrationJob[] jobs) {
        List<Literal> ids=new ArrayList<>();
        for(MigrationJob j:jobs)
            ids.add(Literal.value(j.get_id()));
        return ids.toArray(new Literal[ids.size()]);
    }

    public void cleanupOldJobs(LightblueClient cli,Date cleanupBefore) {
        DataFindRequest findRequest = new DataFindRequest("migrationJob",null);
        // Find completed generated jobs whose jobexecutions don't have a completed job that's close
        Query q=Query.and(Query.withValue("status",Query.eq,MigrationJob.STATE_COMPLETED),
                                    Query.withValue("generated",Query.eq,true),
                                    Query.not(Query.arrayMatch("jobExecutions",
                                                               Query.and(Query.withValue("status",Query.eq,MigrationJob.STATE_COMPLETED),
                                                                         Query.withValue("actualEndDate",Query.gt,new Literal(cleanupBefore))))));
        
        findRequest.where(q);
        LOGGER.debug("Query:{}",q);
        findRequest.select(Projection.includeField("_id"));
        findRequest.range(0,250);
        LOGGER.debug("Cleaning up old jobs");
        for(int loop=0;loop<10;loop++) {
            // We loop at most 10 times to not starve other processes
            try {
                MigrationJob[] jobs=cli.data(findRequest,MigrationJob[].class);
                if(jobs!=null&&jobs.length>0) {
                    LOGGER.debug("Deleting {} jobs",jobs.length);
                    DataDeleteRequest del=new DataDeleteRequest("migrationJob",null);
                    del.where(Query.withValues("_id",Query.in,ids(jobs)));
                    cli.data(del);
                } else
                    break;
            } catch (Exception e) {
                // Log and ignore errors
                LOGGER.error("Error during cleanup",e);
            }
        } 
    }

    public void enableStuckJobs(LightblueClient cli,Date enableBefore) {
        DataFindRequest findRequest=new DataFindRequest("migrationJob",null);
        // Find active jobs that's been sitting for too long
        findRequest.where(Query.and(Query.withValue("status",Query.eq,MigrationJob.STATE_ACTIVE),
                                    Query.arrayMatch("jobExecutions",
                                                     Query.and(Query.withValue("status",Query.eq,MigrationJob.STATE_ACTIVE),
                                                               Query.withValue("actualStartDate",Query.lt,new Literal(enableBefore))))));
        findRequest.select(Projection.includeField("_id"));
        findRequest.range(0,250);
        LOGGER.debug("Re-enabling stuck jobs");
        for(int loop=0;loop<10;loop++) {
            try {
                MigrationJob[] jobs=cli.data(findRequest,MigrationJob[].class);
                if(jobs!=null&&jobs.length>0) {
                    LOGGER.warn("Re-enabling {} active stuck jobs",jobs.length);
                    DataUpdateRequest upd=new DataUpdateRequest("migrationJob",null);
                    upd.where(Query.withValues("_id",Query.in,ids(jobs)));
                    upd.updates(Update.set("status",MigrationJob.STATE_AVAILABLE));
                    LOGGER.debug("update:{}",upd.getBodyJson());
                    cli.data(upd);
                } else
                    break;
            } catch (Exception e) {
                LOGGER.error("Error re-activating jobs",e);
            }
        }
    }
    
    @Override
    public void run() {
        LOGGER.debug("Starting with period:{}",period);
        while(true) {
            try {
                Thread.sleep(period);
            } catch (InterruptedException e) {
                return;
            }

            LOGGER.debug("Woke up");

            LightblueClient cli=controller.getLightblueClient();

            Date d=new Date(System.currentTimeMillis()-oldJobThreshold);
            cleanupOldJobs(cli,d);
            d=new Date(System.currentTimeMillis()-period*3); // 3 times period=too long            
            enableStuckJobs(cli,d);
            
            LOGGER.debug("Completed");
        }        
    }
}
