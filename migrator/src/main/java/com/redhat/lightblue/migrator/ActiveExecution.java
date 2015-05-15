package com.redhat.lightblue.migrator;

/**
 * This class represents an active execution of a migration job. There
 * can only be one active execution for each migration job.
 */
public class ActiveExecution {

    /**
     * The migration job id, also the unique id of active execution
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
}
