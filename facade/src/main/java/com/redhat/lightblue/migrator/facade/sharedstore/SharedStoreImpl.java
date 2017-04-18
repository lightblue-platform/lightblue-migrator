package com.redhat.lightblue.migrator.facade.sharedstore;

import java.net.URL;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SharedStore implementation using ehcache. Creates a cache object per service
 * and uses thread id as key to avoid conflicts. There is an assumption that
 * both legacy and lightblue services create entities in the same order
 * (push/pop).
 *
 * @author mpatercz
 *
 */
public class SharedStoreImpl implements SharedStore {

    private static final Logger log = LoggerFactory.getLogger(SharedStoreImpl.class);

    // singleton
    private CacheManager cacheManager;
    private Cache cache;

    public SharedStoreImpl(String implementationName) {
        this(implementationName, null);
    }

    public SharedStoreImpl(String implementationName, URL ehcacheConfigFile) {
        log.debug("Initializing id cache for " + implementationName);

        if (ehcacheConfigFile == null) {
            cacheManager = CacheManager.create(SharedStoreImpl.class.getResourceAsStream("/ehcache.xml"));
        } else {
            cacheManager = CacheManager.create(ehcacheConfigFile);
        }

        cacheManager.addCacheIfAbsent(implementationName);
        cache = cacheManager.getCache(implementationName);
    }

    @Override
    public void push(Object obj) {
        long threadId = Thread.currentThread().getId();
        if (log.isDebugEnabled()) {
            log.debug("Storing obj=" + obj + " for " + cache.getName() + ", thread=" + threadId);
        }

        Element el = cache.get(threadId);
        LinkedList<Object> list;

        if (el == null) {
            list = new LinkedList<Object>();
        } else {
            list = (LinkedList<Object>) el.getObjectValue();
        }

        list.add(obj);

        cache.put(new Element(threadId, list));
    }

    @Override
    public Object pop() {
        long threadId = Thread.currentThread().getId();
        log.debug("Restoring id for " + cache.getName() + " thread=" + threadId);

        Element el = cache.get(threadId);

        if (el == null) {
            throw new SharedStoreException(cache.getName(), threadId);
        }

        @SuppressWarnings("unchecked")
        LinkedList<Long> list = (LinkedList<Long>) el.getObjectValue();

        try {
            return list.removeFirst();
        } catch (NoSuchElementException e) {
            throw new SharedStoreException(cache.getName(), threadId);
        }
    }

    @Override
    public void copyFromThread(long sourceThreadId) {
        long threadId = Thread.currentThread().getId();

        log.debug("copyFromThread thread=" + sourceThreadId + " to thread=" + threadId);

        Element sourceEl = cache.get(sourceThreadId);

        if (sourceEl != null) {

            @SuppressWarnings("unchecked")
            LinkedList<Object> list = (LinkedList<Object>) sourceEl.getObjectValue();

            // copy by reference
            cache.put(new Element(threadId, list));

            log.debug("Copied key value pairs from thread=" + sourceThreadId + " to thread=" + threadId);
        }

        Element sourceEl2 = cache.get("isDualMigrationPhase-" + sourceThreadId);

        if (sourceEl2 != null) {
            Boolean isDualMigrationPhase = (Boolean) sourceEl2.getObjectValue();

            // copy by reference
            cache.put(new Element("isDualMigrationPhase-" + threadId, isDualMigrationPhase));

            log.debug("Copied isDualMigrationPhase from thread=" + sourceThreadId + " to thread=" + threadId);
        }
    }

    @Override
    public boolean isDualMigrationPhase() {
        long threadId = Thread.currentThread().getId();

        log.debug("Reading isDualMigrationPhase for " + cache.getName() + " thread=" + threadId);

        Element el = cache.get("isDualMigrationPhase-" + threadId);

        if (el == null) {
            throw new SharedStoreException(cache.getName(), threadId);
        }

        return (Boolean) el.getObjectValue();
    }

    public void setDualMigrationPhase(boolean isDualMigrationPhase) {
        long threadId = Thread.currentThread().getId();

        log.debug("Storing isDualMigrationPhase for " + cache.getName() + " thread=" + threadId);

        cache.put(new Element("isDualMigrationPhase-" + threadId, isDualMigrationPhase));
    }

    @Override
    public void clear() {
        long threadId = Thread.currentThread().getId();

        log.debug("Clearing data for " + cache.getName() + " thread=" + threadId);
        cache.remove(threadId);
        cache.remove("isDualMigrationPhase-" + threadId);
    }

    @Override
    public boolean isEmpty() {
        long threadId = Thread.currentThread().getId();

        return (cache.get(threadId) == null);
    }

}
