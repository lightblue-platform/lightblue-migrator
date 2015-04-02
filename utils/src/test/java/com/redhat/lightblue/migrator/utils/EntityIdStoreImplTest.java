package com.redhat.lightblue.migrator.utils;

import net.sf.ehcache.CacheManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class EntityIdStoreImplTest {

    @Test
    public void testSingle() {
        EntityIdStoreImpl store = new EntityIdStoreImpl(EntityIdStoreImplTest.class);

        store.push(101l);
        Assert.assertEquals((Long)101l, store.pop());
    }

    @Test
    public void testList() {
        EntityIdStoreImpl store = new EntityIdStoreImpl(EntityIdStoreImplTest.class);

        store.push(101l);
        store.push(102l);
        store.push(103l);
        Assert.assertEquals((Long)101l, store.pop());
        Assert.assertEquals((Long)102l, store.pop());
        Assert.assertEquals((Long)103l, store.pop());
    }

    @Test
    public void testDifferentCaches() {
        EntityIdStoreImpl store1 = new EntityIdStoreImpl(EntityIdStoreImplTest.class);
        EntityIdStoreImpl store2 = new EntityIdStoreImpl(Country.class);

        store1.push(101l);
        store1.push(102l);
        store2.push(104l);
        store2.push(105l);
        Assert.assertEquals((Long)101l, store1.pop());
        Assert.assertEquals((Long)104l, store2.pop());
        Assert.assertEquals((Long)102l, store1.pop());
        Assert.assertEquals((Long)105l, store2.pop());
    }

    @Test(expected=RuntimeException.class)
    public void noId() {
        EntityIdStoreImpl store = new EntityIdStoreImpl(EntityIdStoreImplTest.class);
        store.pop();
    }

    @Test(expected=RuntimeException.class)
    public void noId2() {
        EntityIdStoreImpl store = new EntityIdStoreImpl(EntityIdStoreImplTest.class);
        store.push(1l);
        store.pop();
        store.pop();
    }

    @After
    public void clearAll() {
        CacheManager.create().clearAll();
    }

}
