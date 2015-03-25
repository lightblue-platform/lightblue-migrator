package com.redhat.lightblue.migrator.utils;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class EntityIdStoreImpl implements EntityIdStore {

    // singleton
    private CacheManager cacheManager = CacheManager.create();
    private Cache cache;

    public EntityIdStoreImpl(Class entityClass) {
        cacheManager.addCacheIfAbsent(entityClass.getCanonicalName());
        cache = cacheManager.getCache(entityClass.getCanonicalName());
    }

    @Override
    public void storeId(Long id) {
        cache.put(new Element(Thread.currentThread().getId(), id));
    }

    @Override
    public Long restoreId() {
        Long key = Thread.currentThread().getId();
        if (cache.isKeyInCache(key)) {
            Element e = cache.get(Thread.currentThread().getId());
            cache.remove(Thread.currentThread().getId());
            return (Long)e.getObjectValue();
        }
        else {
            return null;
        }
    }

}
