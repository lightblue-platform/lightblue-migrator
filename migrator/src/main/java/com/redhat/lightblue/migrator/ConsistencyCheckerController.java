package com.redhat.lightblue.migrator;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormat;
import org.joda.time.Period;

import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Update;
import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

/**
 * There is one consistency checker controller for each
 * entity. Consistency checker controller is created and initialized
 * from a MigrationConfiguration. The migration configuration gives
 * the period with which the consistency checker runs. The consistency
 * checker operates using a timestamp field. Every time it wakes up,
 * it attempts to process documents that were created/modified after
 * the last run time, and before (now-period).  When the consistency
 * checker wakes up, it creates a migration job for every (period)
 * length time slice.
 *
 * The consistency checker always leaves the last period
 * unprocessed. This is to allow for any time discrepancies between
 * nodes updating the database. If a node updates a record with a
 * timestamp that is smaller than the current latest record, the
 * consistency checker will miss that in the next run if the current
 * maximum was used as the starting value of the next run.
 */
public class ConsistencyCheckerController extends AbstractController {

    private static final Logger LOGGER=LoggerFactory.getLogger(ConsistencyCheckerController.class);

    public ConsistencyCheckerController(Controller controller,MigrationConfiguration migrationConfiguration) {
        super(controller,migrationConfiguration,"ConsistencyChecker:"+migrationConfiguration.getConfigurationName());
    }

    /**
     * Returns the period in msecs
     */
    public static long parsePeriod(String periodStr) {
        PeriodFormatter fmt=PeriodFormat.getDefault();
        Period p=fmt.parsePeriod(periodStr);
        return p.toStandardDuration().getMillis();
    }



    /**
     * Returns the end date given the start date end the period. The
     * end date is startDate+period, but only if endDate is at least
     * one period ago. That is, we always leave the last incomplete
     * period unprocessed.
     *
     * Override this to control the job generation algorithm
     */
    public Date getEndDate(Date startDate,long period) {
        long now=getNow().getTime();
        long endDate=startDate.getTime()+period;
        if(now-period>endDate)
            return new Date(endDate);
        else
            return null;
    }

    /**
     * This is simply here so that we can override it in the tests, and change the current time
     */
    protected Date getNow() {
        return new Date();
    }

    /**
     * Create a migration job or jobs to process records created between the
     * given dates. startDate is inclusive, end date is exclusive
     */
    protected List<MigrationJob> createJobs(Date startDate,Date endDate,ActiveExecution ae) throws Exception {
        List<MigrationJob> ret=new ArrayList<MigrationJob>();
        LOGGER.debug("Creating the migrator to setup new jobs");
        // We setup a new migration job
        MigrationJob mj=new MigrationJob();
        mj.setConfigurationName(getMigrationConfiguration().getConfigurationName());
        mj.setScheduledDate(getNow());
        mj.setGenerated(true);
        mj.setStatus(MigrationJob.STATE_AVAILABLE);
        Migrator migrator=createMigrator(mj,ae);
        mj.setQuery(migrator.createRangeQuery(startDate,endDate));
        // At this point, mj.query contains the range query
        LOGGER.debug("Migration job query:{}",mj.getQuery());
        ret.add(mj);
        return ret;
    }

    private void update(List<MigrationJob> mjList) throws Exception {
        DataInsertRequest req=new DataInsertRequest("migrationJob",null);
        LOGGER.debug("Adding {} jobs",mjList.size());
        req.create(mjList);
        lbClient.data(req);
        DataUpdateRequest upd=new DataUpdateRequest("migrationConfiguration",null);
        upd.where(Query.withValue("_id",Query.eq,migrationConfiguration.get_id()));
        upd.updates(Update.set("timestampInitialValue",Literal.value(migrationConfiguration.getTimestampInitialValue())));
        lbClient.data(upd);        
    }

    private boolean migrationJobsExist()  {
        LOGGER.debug("Checking if there are migration jobs for {}",migrationConfiguration.getConfigurationName());
        DataFindRequest req=new DataFindRequest("migrationJob",null);
        req.where(Query.and(Query.withValue("configurationName",Query.eq,migrationConfiguration.getConfigurationName()),
                            Query.withValue("generated",Query.eq,false),
                            Query.withValue("status",Query.eq,MigrationJob.STATE_AVAILABLE)));
        req.select(Projection.includeField("_id"));
        req.range(1,1);
        try {
            LightblueResponse resp=lbClient.data(req);
            return resp.parseMatchCount()>0;
        } catch (Exception e) {
            LOGGER.error("Cannot query migration jobs:{}",e,e);
            return true;
        }
    }
    
    @Override
    public void run() {
        LOGGER.debug("Starting consistency checker controller for {} with period {}",migrationConfiguration.getConfigurationName(),
                     migrationConfiguration.getPeriod());
        boolean interrupted=false;
        long period=parsePeriod(migrationConfiguration.getPeriod());
        while(!interrupted) {
            interrupted=isInterrupted();
            if(!interrupted) {
                Breakpoint.checkpoint("CCC:start");
                LOGGER.debug("Consistency checker {} woke up",migrationConfiguration.getConfigurationName());
                // Lets update our configuration first
                MigrationConfiguration newCfg=reloadMigrationConfiguration();
                if(newCfg==null) {
                    interrupted=true;
                    LOGGER.debug("Consistency checker {} configuration is no longer available",migrationConfiguration.getConfigurationName());
                } else {
                    migrationConfiguration=newCfg;
                    // Lets recalculate period, just in case it changed
                    period=parsePeriod(migrationConfiguration.getPeriod());

                    if(migrationJobsExist()) {
                        LOGGER.info("There are migration jobs for {}, not running consistency checker this time",
                                     migrationConfiguration.getConfigurationName());
                    } else {
                        
                        // We abuse the migration job locking mechanism
                        // here to make sure only one consistency checker
                        // controller is up at any given time This process
                        // only creates migration jobs for the new
                        // periods, so this is not a high-load process,
                        // and we don't need many instances of it
                        // running. But since the migrator can run on
                        // multiple hosts, there is no way to prevent
                        // it. At least, we make sure only one of the
                        // consistencyy checker controllers wakes up at
                        // any given time.
                        
                        // We create a lock using a custom id:
                        String lockId=migrationConfiguration.getConfigurationName()+":ConsistencyCheckerController";
                        ActiveExecution ae;
                        try {
                            ae=lock(lockId);
                        } catch (Exception e) {
                            LOGGER.error("Exception during lock attempt {}:{}",lockId,e);
                            ae=null;
                        }
                        if(ae!=null) {
                            Breakpoint.checkpoint("CCC:locked");
                            LOGGER.debug("This is the only running consistency checker instance for {}",migrationConfiguration.getConfigurationName());
                            try {
                                Date endDate=null;
                                Date startDate= migrationConfiguration.getTimestampInitialValue();
                                if(startDate!=null) {
                                    endDate=getEndDate(startDate,period);
                                    if(endDate==null) {
                                        LOGGER.debug("{} will wait for next period",migrationConfiguration.getConfigurationName());
                                    } else {
                                        Breakpoint.checkpoint("CCC:beforeCreateJobs");
                                        List<MigrationJob> mjList=new ArrayList<>();
                                        do {
                                            LOGGER.debug("{} will create a job for period {}-{}",migrationConfiguration.getConfigurationName(),startDate,endDate);
                                            mjList.addAll(createJobs(startDate,endDate,ae));
                                            migrationConfiguration.setTimestampInitialValue(endDate);
                                            startDate=endDate;
                                            endDate=getEndDate(startDate,period);
                                            interrupted=isInterrupted();
                                        } while(endDate!=null&&!interrupted);
                                        interrupted=isInterrupted();
                                        if(!mjList.isEmpty()&&!interrupted) {
                                            try {
                                                update(mjList);
                                            } catch (Exception e) {
                                                LOGGER.error("Cannot create jobs:{}",e,e);
                                            }
                                        }
                                        LOGGER.debug("Created all the jobs");
                                        Breakpoint.checkpoint("CCC:afterCreateJobs");
                                    }
                                } else
                                    LOGGER.error("Invalid timestamp initial value for {}, skipping this run",migrationConfiguration.getConfigurationName());
                            } catch(Exception e) {
                                LOGGER.error("Error during job creation:{}",e,e);
                            } finally {
                                LOGGER.debug("Unlocking consistency checker {}",migrationConfiguration.getConfigurationName());
                                unlock(lockId);
                            }
                        }
                    }
                }
            }
            if(!interrupted) {
                try {
                    LOGGER.debug("Consistency checker {} is going to sleep for {} msecs",
                                 migrationConfiguration.getConfigurationName(),
                                 period);
                    Thread.sleep(period);
                } catch (InterruptedException e) {
                    interrupted=true;
                }
            }
        }
        Breakpoint.checkpoint("CCC:end");
        LOGGER.debug("Ending controller thread for {}",migrationConfiguration.getConfigurationName());
    }
}
