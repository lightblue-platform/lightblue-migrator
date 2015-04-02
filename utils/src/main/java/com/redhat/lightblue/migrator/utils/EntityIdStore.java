package com.redhat.lightblue.migrator.utils;

/**
 * Used to pass IDs to create apis in Lightblue DAO without changing the signatures.
 *
 * TODO: use a FIFO push/pop instead. This will support creating more than one object in a single api call.
 *
 * @author mpatercz
 *
 */
public interface EntityIdStore {

    public void storeId(Long id);

    public Long restoreId();

}
