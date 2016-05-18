package com.redhat.lightblue.migrator.facade.sharedstore;

public class SharedStoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SharedStoreException(String cacheName, long threadId) {
        super("No objects found for " + cacheName + " thread=" + threadId + "!");
    }

    public SharedStoreException(String cacheName, long threadId, Throwable e) {
        super("No objects found for " + cacheName + " thread=" + threadId + "!", e);
    }

}
