package com.redhat.lightblue.migrator.monitor;

public interface Notifier {

    /**
     * The Monitor is working properly, but the check legitimately failed.
     * @param message
     */
    void sendFailure(String message);

    /**
     * Have a nice day
     */
    void sendSuccess();

    /**
     * A critical error where the Monitor is unable to do it's job correctly.
     * @param message
     */
    void sendError(String message);

}
