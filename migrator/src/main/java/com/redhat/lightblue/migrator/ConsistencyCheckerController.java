package com.redhat.lightblue.migrator;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Update;
import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;

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

    private static final class UOM {
        final long multiplier;
        final String[] names;

        public UOM(long multiplier,String...names) {
            this.names=names;
            this.multiplier=multiplier;
        }

        public boolean in(String s) {
            for(String n:names)
                if(s.equalsIgnoreCase(n))
                    return true;
            return false;
        }
    }

    private static final UOM[] uoms=new UOM[] {
        new UOM(1l,"ms","msec","msecs"),
        new UOM(1000l,"s","sec","secs","second","seconds"),
        new UOM(60l*1000l,"m","min","mins","minute","minutes"),
        new UOM(60l*60l*1000l,"h","hr","hrs","hour","hours"),
        new UOM(24l*60l*60l*1000l,"d","day","days") };
    
    /**
     * Returns the period in msecs
     */
    public static long parsePeriod(String periodStr) {
        // Expect a number, and an optional string
        int n=periodStr.length();
        int index=0;
        for(int i=0;i<n;i++) {
            if(!Character.isDigit(periodStr.charAt(i))&&index==0) {
                index=i;
                break;
            }
        }
        if(index==0)
            return Long.valueOf(periodStr);
        else {
            String number=periodStr.substring(0,index);
            String unit=periodStr.substring(index).trim().toLowerCase();
            Long l=Long.valueOf(number);
            for(UOM u:uoms)
                if(u.in(unit))
                    return l*u.multiplier;
            throw new IllegalArgumentException("period:"+periodStr);
        }
    }



    /**
     * Returns the end date given the start date end the period. The
     * end date is startDate+period, but only if endDate is at least
     * one period ago. That is, we always leave the last incomplete
     * period unprocessed.
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
     * Create a migration job to process records created between the
     * given dates. startDate is inclusive, end date is exclusive
     */
    protected MigrationJob createJob(Date startDate,Date endDate,ActiveExecution ae) throws Exception {
        LOGGER.debug("Creating the migrator to setup new jobs");
        // We setup a new migration job
        MigrationJob mj=new MigrationJob();
        mj.setConfigurationName(getMigrationConfiguration().getConfigurationName());
        mj.setScheduledDate(getNow());
        mj.setStatus(MigrationJob.STATE_AVAILABLE);
        Migrator migrator=createMigrator(mj,ae);
        mj.setQuery(migrator.createRangeQuery(startDate,endDate));
        // At this point, mj.query contains the range query
        LOGGER.debug("Migration job query:{}",mj.getQuery());
        return mj;
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
                        try {
                            LOGGER.debug("This is the only running consistency checker instance for {}",migrationConfiguration.getConfigurationName());
                            
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
                                        mjList.add(createJob(startDate,endDate,ae));
                                        migrationConfiguration.setTimestampInitialValue(endDate);
                                        startDate=endDate;
                                        endDate=getEndDate(startDate,period);
                                        if(mjList.size()>32) {
                                            try {
                                                update(mjList);
                                                mjList.clear();
                                            } catch(Exception e) {
                                                LOGGER.error("Cannot create jobs: {}",e,e);
                                            }
                                        }
                                        interrupted=isInterrupted();
                                    } while(endDate!=null&&!interrupted);
                                    if(!mjList.isEmpty()) {
                                        try {
                                            update(mjList);
                                        } catch (Exception e) {
                                            LOGGER.error("Cannot create jobs:{}",e,e);
                                        }
                                    }
                                    Breakpoint.checkpoint("CCC:afterCreateJobs");
                                }
                            }else
                                LOGGER.error("Invalid timestamp initial value for {}, skipping this run",migrationConfiguration.getConfigurationName());
                        } catch(Exception e) {
                            LOGGER.error("Error during job creation:{}",e,e);
                        } finally {
                            unlock(lockId);
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
