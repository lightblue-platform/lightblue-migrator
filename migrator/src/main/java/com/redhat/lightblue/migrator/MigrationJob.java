package com.redhat.lightblue.migrator;

import java.util.Date;

public class MigrationJob {

    /**
     * The migration job id
     */
    private String migrationJobId;

    private String configurationName;

    /**
     * Date job is scheduled to run
     */
    private Date scheduledDate;

    /**
     * A back-end specific query to retrieve documents that will be migrated by this job
     */
    private String query;

    /**
     * Entity to be migrated
     */
    private String entityName;
    private String entityVersion;

    private String state; // available, processing, complete

    public String getMigrationJobId() {
        return migrationJobId;
    }

    public void setMigrationJobId(String s) {
        migrationJobId=s;
    }

    public String getConfigurationName() {
        return configurationName;
    }

    public void setConfigurationName(String s) {
        configurationName=s;
    }

    public Date getScheduledDate() {
        return scheduledDate;
    }

    public void setScheduledDate(Date d) {
        scheduledDate=d;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String s) {
        query=s;
    }


    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String s) {
        entityName=s;
    }

    public String getEntityVersion() {
        return entityVersion;
    }

    public void setEntityVersion(String s) {
        entityVersion=s;
    }
}
