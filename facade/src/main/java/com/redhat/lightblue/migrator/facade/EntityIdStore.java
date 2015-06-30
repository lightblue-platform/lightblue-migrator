package com.redhat.lightblue.migrator.facade;

/**
 * A FIFO queue. Used to pass IDs to create apis in Lightblue DAO without changing the signatures.
 *
 *
 * @author mpatercz
 *
 */
public interface EntityIdStore {

    public void push(Long id);

    public Long pop();

    /**
     * Copy all key-value pairs from one thread to the other.
     *
     */
    public void copyFromThread(long sourceThreadId);

}
