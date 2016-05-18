package com.redhat.lightblue.migrator.facade;

@Deprecated
public class EntityIdStoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EntityIdStoreException(Throwable t) {
        super(t);
    }

    public EntityIdStoreException(String cacheName, long threadId) {
        super("No ids found for " + cacheName + " thread=" + threadId + "!");
    }

    public EntityIdStoreException(String cacheName, long threadId, Throwable e) {
        super("No ids found for " + cacheName + " thread=" + threadId + "!", e);
    }

}
