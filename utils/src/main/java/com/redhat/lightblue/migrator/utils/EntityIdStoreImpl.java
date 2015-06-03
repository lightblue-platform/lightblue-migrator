package com.redhat.lightblue.migrator.utils;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityIdStore implementation using ehcache. Creates a cache object per dao and uses thread id as key to avoid conflicts.
 * There is an assumption that both legacy and destination daos create entities in the same order.
 *
 * TODO: ehcache.xml will need to be optimized to minimize overhead.
 *
 * @author mpatercz
 *
 */
public class EntityIdStoreImpl implements EntityIdStore {

    private static final Logger log = LoggerFactory.getLogger(EntityIdStoreImpl.class);

    // singleton
    private CacheManager cacheManager = CacheManager.create();
    private Cache cache;

    public EntityIdStoreImpl(Class<?> daoClass) {
        log.debug("Initializing id cache for "+daoClass.getCanonicalName());
        cacheManager.addCacheIfAbsent(daoClass.getCanonicalName());
        cache = cacheManager.getCache(daoClass.getCanonicalName());
    }

    @Override
    public void push(Long id) {
        long threadId = Thread.currentThread().getId();
        if(log.isDebugEnabled())
            log.debug("Storing id="+id+" for "+cache.getName()+", thread="+threadId);
        if (!cache.isKeyInCache(threadId)) {
            cache.put(new Element(threadId, new LinkedList<Long>()));
        }

        if (log.isDebugEnabled())
            log.debug("cache("+threadId+")="+cache.get(threadId));

        @SuppressWarnings("unchecked")
        LinkedList<Long> list = (LinkedList<Long>)cache.get(threadId).getObjectValue();

        list.add(id);
    }

    @Override
    public Long pop() {
        long threadId = Thread.currentThread().getId();
        log.debug("Restoring id for "+cache.getName()+" thread="+threadId);
        if (!cache.isKeyInCache(threadId)) {
            throw new RuntimeException("No ids found for "+cache.getName()+" thread="+threadId+"!");
        }

        @SuppressWarnings("unchecked")
        LinkedList<Long> list = (LinkedList<Long>)cache.get(threadId).getObjectValue();

        try {
            return list.removeFirst();
        } catch (NoSuchElementException e) {
            throw new RuntimeException("No ids found for "+cache.getName()+" thread="+threadId+"!", e);
        }
    }

}
