package com.redhat.lightblue.migrator.facade.sharedstore;

import net.sf.ehcache.CacheManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStore;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreImpl;

public class SharedStoreTest {

    @Test
    public void testSingle() {
        SharedStore store = new SharedStoreImpl(SharedStoreTest.class);

        store.push(101l);
        Assert.assertEquals((Long)101l, store.pop());
    }

    @Test
    public void testList() {
        SharedStore store = new SharedStoreImpl(SharedStoreTest.class);

        store.push(101l);
        store.push(102l);
        store.push(103l);
        Assert.assertEquals((Long)101l, store.pop());
        Assert.assertEquals((Long)102l, store.pop());
        Assert.assertEquals((Long)103l, store.pop());
    }

    @Test
    public void testDifferentCaches() {
        SharedStore store1 = new SharedStoreImpl(SharedStoreTest.class);
        SharedStore store2 = new SharedStoreImpl(Country.class);

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
        SharedStore store = new SharedStoreImpl(SharedStoreTest.class);
        store.pop();
    }

    @Test(expected=RuntimeException.class)
    public void noId2() {
        SharedStore store = new SharedStoreImpl(SharedStoreTest.class);
        store.push(1l);
        store.pop();
        store.pop();
    }

    @Test
    public void testCopy() {
        SharedStore store = new SharedStoreImpl(SharedStoreTest.class);

        store.push(101l);
        store.push(102l);
        store.push(103l);
        store.setDualMigrationPhase(true);

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

    @Test
    public void testObject() {
        SharedStore store = new SharedStoreImpl(SharedStoreTest.class);

        store.push("foo");
        store.push("bar");
        store.push("foobar");

        Assert.assertEquals("foo", store.pop());
        Assert.assertEquals("bar", store.pop());
        Assert.assertEquals("foobar", store.pop());
    }

    @Test
    public void testIsDualMigrationPhase() {
        SharedStoreImpl store = new SharedStoreImpl(SharedStoreTest.class);

        try {
            store.isDualMigrationPhase();
            Assert.fail();
        } catch (SharedStoreException e) {

        }

        store.setDualMigrationPhase(true);
        Assert.assertTrue(store.isDualMigrationPhase());
        store.setDualMigrationPhase(false);
        Assert.assertFalse(store.isDualMigrationPhase());
    }

    class TestThread extends Thread {

        private SharedStore store;
        private Long parentThreadId;
        private boolean checksPassed = false;

        public TestThread(SharedStore store, Long parentThreadId) {
            super();
            this.store = store;
            this.parentThreadId = parentThreadId;
        }

        @Override
        public void run() {
            store.copyFromThread(parentThreadId);

            checksPassed = 101l == (Long)store.pop() && 102l == (Long)store.pop() && 103l == (Long)store.pop() &&
                    store.isDualMigrationPhase();
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
