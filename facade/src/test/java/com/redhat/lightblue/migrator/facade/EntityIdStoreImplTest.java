package com.redhat.lightblue.migrator.facade;

import net.sf.ehcache.CacheManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.redhat.lightblue.migrator.facade.EntityIdStoreImpl;
import com.redhat.lightblue.migrator.facade.model.Country;

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

    @Test
    public void testCopy() {
        EntityIdStoreImpl store = new EntityIdStoreImpl(EntityIdStoreImplTest.class);

        store.push(101l);
        store.push(102l);
        store.push(103l);

        TestThread t = new TestThread(store, Thread.currentThread().getId());

        t.start();
        try {
            t.join();
            Assert.assertTrue(t.isChecksPassed());
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    class TestThread extends Thread {

        private EntityIdStore store;
        private Long parentThreadId;
        private boolean checksPassed = false;

        public TestThread(EntityIdStore store, Long parentThreadId) {
            super();
            this.store = store;
            this.parentThreadId = parentThreadId;
        }

        @Override
        public void run() {
            store.copyFromThread(parentThreadId);

            checksPassed = 101l == store.pop() && 102l == store.pop() && 103l == store.pop();
        }

        public boolean isChecksPassed() {
            return checksPassed;
        }
    }

    @After
    public void clearAll() {
        CacheManager.create().clearAll();
    }

}
