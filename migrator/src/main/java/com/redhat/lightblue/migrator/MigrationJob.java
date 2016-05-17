package com.redhat.lightblue.migrator;

import java.util.Date;
import java.util.List;

public class MigrationJob {

    public static final String ENTITY_NAME = "migrationJob";

    public static final String STATE_AVAILABLE = "available";
    public static final String STATE_ACTIVE = "active";
    public static final String STATE_COMPLETED = "completed";
    public static final String STATE_FAILED = "failed";

    public static class ConsistencyChecker {
        /**
         * Beginning of job range, date, id, etc.
         */
        private String jobRangeBegin;

        /**
         * End of job range, date, id, etc.
         */
        private String jobRangeEnd;

        private String configurationName;

        public String getConfigurationName() {
            return configurationName;
        }

        public void setConfigurationName(String s) {
            configurationName = s;
        }

        public String getJobRangeBegin() {
            return jobRangeBegin;
        }

        public void setJobRangeBegin(String s) {
            jobRangeBegin = s;
        }

        public String getJobRangeEnd() {
            return jobRangeEnd;
        }

        public void setJobRangeEnd(String s) {
            jobRangeEnd = s;
        }

    }

    public static class JobExecution {
        private String activeExecutionId;
        private String ownerName;
        private String hostName;
        private Date actualStartDate;
        private Date actualEndDate;
        private String status;
        private String errorMsg;
        private int processedDocumentCount;
        private int consistentDocumentCount;
        private int inconsistentDocumentCount;
        private int overwrittenDocumentCount;

        /**
         * Gets the value of activeExecutionId
         *
         * @return the value of activeExecutionId
         */
        public final String getActiveExecutionId() {
            return activeExecutionId;
        }

        /**
         * Sets the value of activeExecutionId
         *
         * @param argActiveExecutionId Value to assign to this.activeExecutionId
         */
        public final void setActiveExecutionId(final String argActiveExecutionId) {
            activeExecutionId = argActiveExecutionId;
        }

        /**
         * Gets the value of ownerName
         *
         * @return the value of ownerName
         */
        public final String getOwnerName() {
            return ownerName;
        }

        /**
         * Sets the value of ownerName
         *
         * @param argOwnerName Value to assign to this.ownerName
         */
        public final void setOwnerName(final String argOwnerName) {
            ownerName = argOwnerName;
        }

        /**
         * Gets the value of hostName
         *
         * @return the value of hostName
         */
        public final String getHostName() {
            return hostName;
        }

        /**
         * Sets the value of hostName
         *
         * @param argHostName Value to assign to this.hostName
         */
        public final void setHostName(final String argHostName) {
            hostName = argHostName;
        }

        /**
         * Gets the value of actualStartDate
         *
         * @return the value of actualStartDate
         */
        public final Date getActualStartDate() {
            return actualStartDate;
        }

        /**
         * Sets the value of actualStartDate
         *
         * @param argActualStartDate Value to assign to this.actualStartDate
         */
        public final void setActualStartDate(final Date argActualStartDate) {
            actualStartDate = argActualStartDate;
        }

        /**
         * Gets the value of actualEndDate
         *
         * @return the value of actualEndDate
         */
        public final Date getActualEndDate() {
            return actualEndDate;
        }

        /**
         * Sets the value of actualEndDate
         *
         * @param argActualEndDate Value to assign to this.actualEndDate
         */
        public final void setActualEndDate(final Date argActualEndDate) {
            actualEndDate = argActualEndDate;
        }

        /**
         * Gets the value of status
         *
         * @return the value of status
         */
        public final String getStatus() {
            return status;
        }

        /**
         * Sets the value of status
         *
         * @param argStatus Value to assign to this.status
         */
        public final void setStatus(final String argStatus) {
            status = argStatus;
        }

        /**
         * Gets the value of errorMsg
         *
         * @return the value of errorMsg
         */
        public final String getErrorMsg() {
            return errorMsg;
        }

        /**
         * Sets the value of errorMsg
         *
         * @param argErrorMsg Value to assign to this.errorMsg
         */
        public final void setErrorMsg(final String argErrorMsg) {
            errorMsg = argErrorMsg;
        }

        /**
         * Gets the value of processedDocumentCount
         *
         * @return the value of processedDocumentCount
         */
        public final int getProcessedDocumentCount() {
            return processedDocumentCount;
        }

        /**
         * Sets the value of processedDocumentCount
         *
         * @param argProcessedDocumentCount Value to assign to
         * this.processedDocumentCount
         */
        public final void setProcessedDocumentCount(final int argProcessedDocumentCount) {
            processedDocumentCount = argProcessedDocumentCount;
        }

        /**
         * Gets the value of consistentDocumentCount
         *
         * @return the value of consistentDocumentCount
         */
        public final int getConsistentDocumentCount() {
            return consistentDocumentCount;
        }

        /**
         * Sets the value of consistentDocumentCount
         *
         * @param argConsistentDocumentCount Value to assign to
         * this.consistentDocumentCount
         */
        public final void setConsistentDocumentCount(final int argConsistentDocumentCount) {
            consistentDocumentCount = argConsistentDocumentCount;
        }

        /**
         * Gets the value of inconsistentDocumentCount
         *
         * @return the value of inconsistentDocumentCount
         */
        public final int getInconsistentDocumentCount() {
            return inconsistentDocumentCount;
        }

        /**
         * Sets the value of inconsistentDocumentCount
         *
         * @param argInconsistentDocumentCount Value to assign to
         * this.inconsistentDocumentCount
         */
        public final void setInconsistentDocumentCount(final int argInconsistentDocumentCount) {
            inconsistentDocumentCount = argInconsistentDocumentCount;
        }

        /**
         * Gets the value of overwrittenDocumentCount
         *
         * @return the value of overwrittenDocumentCount
         */
        public final int getOverwrittenDocumentCount() {
            return overwrittenDocumentCount;
        }

        /**
         * Sets the value of overwrittenDocumentCount
         *
         * @param argOverwrittenDocumentCount Value to assign to
         * this.overwrittenDocumentCount
         */
        public final void setOverwrittenDocumentCount(final int argOverwrittenDocumentCount) {
            overwrittenDocumentCount = argOverwrittenDocumentCount;
        }

    }

    /**
     * The migration job id
     */
    private String _id;

    private String configurationName;

    /**
     * Date job is scheduled to run
     */
    private Date scheduledDate;

    /**
     * A back-end specific query to retrieve documents that will be migrated by
     * this job
     */
    private String query;

    /**
     * available, processing, complete, failed
     */
    private String status;

    /**
     * Set if generated by consistency checker
     */
    private boolean generated;

    private ConsistencyChecker consistencyChecker;

    private List<JobExecution> jobExecutions;

    public String get_id() {
        return _id;
    }

    public void set_id(String s) {
        _id = s;
    }

    public String getConfigurationName() {
        return configurationName;
    }

    public void setConfigurationName(String s) {
        configurationName = s;
    }

    public Date getScheduledDate() {
        return scheduledDate;
    }

    public void setScheduledDate(Date d) {
        scheduledDate = d;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String s) {
        query = s;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String s) {
        status = s;
    }

    public boolean isGenerated() {
        return generated;
    }

    public void setGenerated(boolean s) {
        generated = s;
    }

    public ConsistencyChecker getConsistencyChecker() {
        return consistencyChecker;
    }

    public void setConsistencyChecker(ConsistencyChecker c) {
        consistencyChecker = c;
    }

    public List<JobExecution> getJobExecutions() {
        return jobExecutions;
    }

    public void setJobExecutions(List<JobExecution> a) {
        jobExecutions = a;
    }
}
