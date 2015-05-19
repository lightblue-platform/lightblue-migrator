package com.redhat.lightblue.migrator;

import java.util.Date;

public class MigrationJobExecution {

    private String activeExecutionId;
    // actual run times for job
    private Date actualStartDate;
    private Date actualEndDate;
    
    public String sourceQuery;

    private String status;

    // summary info on what the job did
    private int processedDocumentCount = 0;
    private int consistentDocumentCount = 0;
    private int inconsistentDocumentCount = 0;
    private int overwrittenDocumentCount = 0;

    public String getActiveExecutionId() {
        return activeExecutionId;
    }

    public void setActiveExecutionId(String s) {
        activeExecutionId=s;
    }
    
    public Date getActualStartDate() {
        return actualStartDate;
    }

    public void setActualStartDate(Date actualStartDate) {
        this.actualStartDate = actualStartDate;
    }

    public Date getActualEndDate() {
        return actualEndDate;
    }

    public void setActualEndDate(Date actualEndDate) {
        this.actualEndDate = actualEndDate;
    }
    
    public String getSourceQuery() {
        return sourceQuery;
    }
    
    public void setSourceQuery(String sourceQuery) {
        this.sourceQuery = sourceQuery;
    }

    public int getProcessedDocumentCount() {
        return processedDocumentCount;
    }

    public void setProcessedDocumentCount(int processedDocumentCount) {
        this.processedDocumentCount = processedDocumentCount;
    }

    public int getConsistentDocumentCount() {
        return consistentDocumentCount;
    }

    public void setConsistentDocumentCount(int consistentDocumentCount) {
        this.consistentDocumentCount = consistentDocumentCount;
    }

    public int getInconsistentDocumentCount() {
        return inconsistentDocumentCount;
    }

    public void setInconsistentDocumentCount(int inconsistentDocumentCount) {
        this.inconsistentDocumentCount = inconsistentDocumentCount;
    }

    public int getOverwrittenDocumentCount() {
        return overwrittenDocumentCount;
    }

    public void setOverwrittenDocumentCount(int overwrittenDocumentCount) {
        this.overwrittenDocumentCount = overwrittenDocumentCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String jobStatus) {
        this.status = jobStatus;
    }
}
