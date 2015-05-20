package com.redhat.lightblue.migrator;

import java.util.Date;

/**
 * This class represents an active execution of a migration job. There
 * can only be one active execution for each migration job.
 *
 * Instances of this object is used as synchronization aid. A thread
 * willing to process a migration job creates an active execution in
 * the database. The database is setup with a unique index on
 * migrationJobId. If creation is successfull, the thread acquires the
 * job, otherwise the thread looks for another job. So, an
 * ActiveExecution instance is also a lock record for the
 * corresponsing migration job.
 */
public class ActiveExecution {

    private String _id;

    /**
     * The migration job id, there must be a unique index on this field
     */
    private String migrationJobId;

    /**
     * Time when execution started
     */
    private Date startTime;

    /**
     * Number of documents that will be processed by this job
     */
    private int numDocsToProcess;

    /**
     * Number of documents already processed by this job
     */
    private int numDocsProcessed;

    private Date ping;

    public String get_id() {
        return _id;
    }

    public void set_id(String id) {
        _id=id;
    }

    public String getMigrationJobId() {
        return migrationJobId;
    }

    public void setMigrationJobId(String s) {
        migrationJobId=s;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date d) {
        startTime=d;
    }

    public int getNumDocsToProcess() {
        return numDocsToProcess;
    }

    public void setNumDocsToProcess(int i) {
        numDocsToProcess=i;
    }

    public int getNumDocsProcessed() {
        return numDocsProcessed;
    }

    public void setNumDocsProcessed(int i) {
        numDocsProcessed=i;
    }

    /**
     * Gets the value of ping
     *
     * @return the value of ping
     */
    public final Date getPing() {
        return this.ping;
    }

    /**
     * Sets the value of ping
     *
     * @param argPing Value to assign to this.ping
     */
    public final void setPing(final Date argPing) {
        this.ping = argPing;
    }

}
