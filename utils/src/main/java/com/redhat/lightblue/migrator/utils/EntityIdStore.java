package com.redhat.lightblue.migrator.utils;

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

}
