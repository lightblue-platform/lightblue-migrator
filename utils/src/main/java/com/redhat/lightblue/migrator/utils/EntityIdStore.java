package com.redhat.lightblue.migrator.utils;

/**
 * Used to pass IDs to create apis in Lightblue DAO without changing the signatures.
 *
 * @author mpatercz
 *
 */
public interface EntityIdStore {

    public void storeId(Long id);

    public Long restoreId();

}
