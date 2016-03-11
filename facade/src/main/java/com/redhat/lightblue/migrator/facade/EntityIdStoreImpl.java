package com.redhat.lightblue.migrator.facade;

import java.net.URL;

import com.redhat.lightblue.migrator.facade.sharedstore.SharedStore;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreException;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreImpl;

/**
 * Deprecated. Use {@link SharedStoreImpl} instead.
 *
 * EntityIdStore implementation using ehcache. Creates a cache object per dao and uses thread id as key to avoid conflicts.
 * There is an assumption that both legacy and destination daos create entities in the same order.
 *
 * TODO: ehcache.xml will need to be optimized to minimize overhead.
 *
 *
 * @author mpatercz
 *
 */
@Deprecated
public class EntityIdStoreImpl implements EntityIdStore {

    private SharedStore sharedStore;

    public EntityIdStoreImpl(Class<?> daoClass) {
        sharedStore = new SharedStoreImpl(daoClass.getCanonicalName());
    }

    public EntityIdStoreImpl(Class<?> daoClass, URL ehcacheConfigFile) {
        sharedStore = new SharedStoreImpl(daoClass.getCanonicalName(), ehcacheConfigFile);
    }

    @Override
    public void push(Long id) {
        try {
            sharedStore.push(id);
        } catch (SharedStoreException e) {
            throw new EntityIdStoreException(e);
        }
    }

    @Override
    public Long pop() {
        try {
            return (Long)sharedStore.pop();
        } catch (SharedStoreException e) {
            throw new EntityIdStoreException(e);
        }
    }

    @Override
    public void copyFromThread(long sourceThreadId) {
        sharedStore.copyFromThread(sourceThreadId);
    }

    @Override
    public void clear() {
        sharedStore.clear();
    }

}
