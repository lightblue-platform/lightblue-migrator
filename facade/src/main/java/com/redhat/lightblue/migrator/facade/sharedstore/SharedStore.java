package com.redhat.lightblue.migrator.facade.sharedstore;

/**
 * Used for sharing state between legacy and lightblue services.
 *
 * @author mpatercz
 *
 */
public interface SharedStore {

    /**
     * A FIFO queue push.
     */
    public void push(Object obj);

    /**
     * A FIFO queue pop.
     */
    public Object pop();

    /**
     * Copy all key-value pairs from one thread to the other.
     *
     */
    public void copyFromThread(long sourceThreadId);

    /**
     * This flag tells lightblue service implementation if it's running alone or not.
     *
     * @return true if 2 services are running - legacy and lightblue. False if it's only lightblue.
     */
    public boolean isDualMigrationPhase();

    /**
     * See {@link SharedStore#isDualMigrationPhase()}.
     *
     * @param isDualMigrationPhase
     */
    public void setDualMigrationPhase(boolean isDualMigrationPhase);

    /**
     * Clear data for current thread.
     *
     */
    public void clear();

    /**
     * @return true if a value is currently cached, otherwise false.
     */
    public boolean isEmpty();

}
