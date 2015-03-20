package com.redhat.lightblue.migrator.consistency;

/**
 *
 * JobStatus can be one of the following values:
 *      NEW                - Means the Job just created and it isn't ready to be executed
 *      READY              - Means the Job is ready to be executed but not in execution
 *      STARTING           - It just got to be processing
 *      RUNNING            - Currently being processed for synchronous request
 *      RUNNING_ASYNC      - Currently being processed for asynchronous request
 *      FINISHING          - The execution is almost complete, just few more steps to move to a final status
 *      COMPLETED_SUCCESS  - Final status - Execution completed successfully
 *      COMPLETED_PARTIAL  - Final status - Execution completed but only partially (at least one part of the request was successfully processed and at least one part of the request failed )
 *      COMPLETED_FAILED   - Final status - Execution completed but failed (for know exceptions like bad request, entity doesn't exist, etc)
 *      ABORTING           - Forcing the execution to abort
 *      ABORTED_DUPLICATE  - Final status - Execution aborted because it was duplicated
 *      ABORTED_TIMEOUT    - Final status - Execution aborted because of timeout (running too long) (can't be due some lightblue constraint or external constraints like MongoDB server)
 *      ABORTED_AUTH       - Final status - Execution aborted due authentication/authorization issues
 *      ABORTED_UNKNOWN    - Final status - Execution aborted due another problem not listed here and lightblue was able to identify (maybe an external issue not mapped)
 *      UNKNOWN            - Final status - Execution lost is on a non-identifiable state
 *
 * Created by lcestari on 3/19/15.
 */
public enum JobStatus {
    NEW,READY,STARTING,RUNNING,RUNNING_ASYNC,FINISHING,COMPLETED_SUCCESS,COMPLETED_PARTIAL,
    COMPLETED_FAILED,ABORTING,ABORTED_DUPLICATE,ABORTED_TIMEOUT,ABORTED_AUTH,ABORTED_UNKNOWN,UNKNOWN;

    /**
     * If the job is running (represented by the states: STARTING, RUNNING and RUNNING_ASYNC ) returns true. If it is FINISHING it is considered that
     * it is in the completion phase so it is not running anymore.
     *
     * @return true if the job is running, false otherwise
     */
    public boolean isRunning() {
        if(this.equals(STARTING) || this.equals(RUNNING) || this.equals(RUNNING_ASYNC)){
            return true;
        }
        return false;
    }

    /**
     * If the JobStatus is COMPLETED_FAILED or one of the ABORTED or UNKNOWN, it will return true
     * @return true if the job is/was unsuccessful, false otherwise
     */
    public boolean isUnsuccessful(){
        if(this.equals(COMPLETED_FAILED) || this.equals(ABORTED_DUPLICATE) || this.equals(ABORTED_AUTH) || this.equals(ABORTED_TIMEOUT) || this.equals(ABORTED_UNKNOWN) ||
                this.equals(UNKNOWN)){
            return true;
        }
        return false;
    }

    /**
     * If the job is not running anymore, so it must be in one of the final status (so this method is an alias to isFinalStatus())
     *
     * @return true if the job finished, false otherwise
     */
    public boolean isCompleted() {
        return isFinalStatus();
    }

    /**
     * Check sanity of the JobStatus in case it was expected to have a final status (such as completed or aborted or etc) but it isn't. If it is the case, we recommend to change the status to UNKNOWN
     * @return true iff it is one of the documented final status, false otherwise
     */
    public boolean isFinalStatus() {
        if(this.equals(COMPLETED_SUCCESS) || this.equals(COMPLETED_PARTIAL) ||
                this.equals(COMPLETED_FAILED) || this.equals(ABORTED_DUPLICATE) || this.equals(ABORTED_AUTH) || this.equals(ABORTED_TIMEOUT) || this.equals(ABORTED_UNKNOWN) ||
                this.equals(UNKNOWN)){
            return true;
        }
        return false;
    }
}
