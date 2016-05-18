package com.redhat.lightblue.migrator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadMonitor extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadMonitor.class);

    private long threadTimeout = 10l * 60l * 1000l; // 10 minutes timeout

    private long wakeupInterval = 60l * 1000l; // Wake up every minute

    private final List<ThreadStatusListener> statusListeners = new ArrayList<>();

    public enum Status {
        alive, killed, abandoned
    };

    private class ThreadStatus {
        final MonitoredThread thread;

        long lastPing = System.currentTimeMillis();
        String lastPingMsg;
        long killTime = 0;
        Status status;

        public ThreadStatus(MonitoredThread t) {
            this.thread = t;
            this.status = Status.alive;
        }

        public void kill() {
            status = Status.killed;
            killTime = System.currentTimeMillis();
            ((Thread) thread).interrupt();
            threadStatusChanged(thread);
        }

        /**
         * Returns true if thread was alive before, and now it timed out
         */
        public boolean timeout() {
            if (status == Status.alive) {
                if (System.currentTimeMillis() - lastPing > threadTimeout) {
                    kill();
                    return true;
                }
            }
            return false;
        }

        /**
         * Returns true if thread is killed before and now it is abandoned
         */
        public boolean abandon() {
            if (status == Status.killed && killTime > 0) {
                if (System.currentTimeMillis() - killTime > threadTimeout) {
                    status = Status.abandoned;
                    ((Thread) thread).interrupt();
                    threadStatusChanged(thread);
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return "Thread=" + thread + ", lastPing:" + (new Date(lastPing).toString()) + ", status=" + status
                    + " msg=" + lastPingMsg + (killTime > 0 ? ", killlTime:" + (new Date(killTime)) : "");
        }

    }

    private final Map<MonitoredThread, ThreadStatus> threadMap = new HashMap<>();

    public ThreadMonitor(long threadTimeout) {
        this.threadTimeout = threadTimeout;
    }

    public ThreadMonitor() {
    }

    /**
     * Registers a thread status listener
     */
    public synchronized void registerThreadStatusListener(ThreadStatusListener listener) {
        statusListeners.add(listener);
    }

    /**
     * Unregisters a thread status listener
     */
    public synchronized void unregisterThreadStatusListener(ThreadStatusListener listener) {
        statusListeners.remove(listener);
    }

    protected void threadStatusChanged(MonitoredThread thread) {
        ArrayList<ThreadStatusListener> l;
        synchronized (this) {
            l = new ArrayList<>(statusListeners);
        }
        for (ThreadStatusListener x : l) {
            x.threadStatusChanged(thread, this);
        }
    }

    public long getThreadTimeout() {
        return threadTimeout;
    }

    public void ping(String msg) {
        Thread currentThread = Thread.currentThread();
        LOGGER.debug("Ping from {}: {}", currentThread, msg);
        if (currentThread instanceof MonitoredThread) {
            MonitoredThread thread = (MonitoredThread) currentThread;
            ThreadStatus status = threadMap.get(thread);
            if (status == null) {
                synchronized (this) {
                    if (!threadMap.containsKey(thread)) {
                        threadMap.put(thread, status = new ThreadStatus(thread));
                    } else {
                        status = threadMap.get(thread);
                    }
                }
            }
            status.lastPingMsg = msg;
            LOGGER.debug("Status:{}", status);
            long now = System.currentTimeMillis();
            if (status.status == Status.killed) {
                currentThread.interrupt();
                throw new RuntimeException("Thread is killed:" + currentThread);
            }
            synchronized (status) {
                if (status.timeout() || status.abandon()) {
                    throw new RuntimeException("Thread timed out:" + currentThread);
                }
                status.lastPing = now;
            }
        }
    }

    public void endThread() {
        Thread currentThread = Thread.currentThread();
        LOGGER.debug("End thread {}", currentThread);
        if (currentThread instanceof MonitoredThread) {
            MonitoredThread t = (MonitoredThread) currentThread;
            synchronized (this) {
                threadMap.remove(t);
            }
        }
    }

    private synchronized void reap(Map<MonitoredThread, ThreadStatus> threads) {
        List<MonitoredThread> dead = new ArrayList<>();
        for (MonitoredThread t : threads.keySet()) {
            if (!((Thread) t).isAlive()) {
                dead.add(t);
            }
        }
        for (MonitoredThread t : dead) {
            threads.remove(t);
        }
    }

    public synchronized Status getStatus(MonitoredThread t) {
        ThreadStatus s = threadMap.get(t);
        if (s != null) {
            return s.status;
        } else {
            return null;
        }
    }

    /**
     * Returns threads in an array. Some elements may be null.
     */
    private static Thread[] getThreads(ThreadGroup g) {
        int mul = 1;
        do {
            Thread[] arr = new Thread[g.activeCount() * mul + 1];
            if (g.enumerate(arr) < arr.length) {
                return arr;
            }
            mul++;
        } while (true);
    }

    /**
     * Return the count of threads that are in any of the statuses
     */
    public int getThreadCount(ThreadGroup group, Status... s) {
        Thread[] threads = getThreads(group);
        int count = 0;
        for (Thread t : threads) {
            if (t instanceof MonitoredThread) {
                Status status = getStatus((MonitoredThread) t);
                if (status != null) {
                    for (Status x : s) {
                        if (x == status) {
                            count++;
                        }
                    }
                }
            }
        }
        return count;
    }

    public int getThreadCount(Status... s) {
        Map<MonitoredThread, ThreadStatus> snapshot;
        synchronized (this) {
            snapshot = new HashMap<>(threadMap);
        }
        int count = 0;
        for (ThreadStatus ts : snapshot.values()) {
            for (Status x : s) {
                if (x == ts.status) {
                    count++;
                }
            }
        }
        return count;
    }

    private Object waiter = new Object();
    private boolean runNow = false;

    public void runNow() {
        synchronized (waiter) {
            waiter.notifyAll();
        }
        runNow = true;
    }

    @Override
    public void run() {
        LOGGER.debug("Starting with thread timeout:{}", threadTimeout);
        // Wake up every minute, look at all registered threads
        // No exceptions can be thrown from this code
        int abandonedThreadCount = 0;
        while (true) {
            // Wait for a minute
            synchronized (waiter) {
                if (runNow) {
                    runNow = false;
                } else {
                    try {
                        waiter.wait(wakeupInterval);
                    } catch (Exception e) {
                    }
                }
            }
            LOGGER.debug("Running thread monitor after {} msec", threadTimeout);
            Breakpoint.checkpoint("ThreadMonitor:check");
            // Take a snapshot
            Map<MonitoredThread, ThreadStatus> snapshot;
            synchronized (this) {
                reap(threadMap);
                snapshot = new HashMap<>(threadMap);
            }
            LOGGER.debug("Thread snapshot:{}", snapshot);
            long now = System.currentTimeMillis();
            // Check threads
            List<ThreadStatus> unresponsive = new ArrayList<>();
            for (Map.Entry<MonitoredThread, ThreadStatus> entry : snapshot.entrySet()) {
                MonitoredThread thread = entry.getKey();
                ThreadStatus status = entry.getValue();
                if (status.timeout()) {
                    unresponsive.add(status);
                    LOGGER.error("Thread timed out, thread:{}", status);
                } else if (status.abandon()) {
                    unresponsive.add(status);
                    LOGGER.error("Thread abandoned, thread:{}", status);
                }
            }
            if (!unresponsive.isEmpty()) {
                for (ThreadStatus x : unresponsive) {
                    LOGGER.error("Unresponsive thread:{}", x);
                }
            }
            abandonedThreadCount++;
            if (abandonedThreadCount > 10) {
                abandonedThreadCount = 0;
                int n = 0;
                for (Map.Entry<MonitoredThread, ThreadStatus> entry : snapshot.entrySet()) {
                    if (entry.getValue().status == Status.abandoned) {
                        n++;
                    }
                }
                if (n > 0) {
                    LOGGER.error("ALERT: There are {} abandoned threads", n);
                }
            }
        }
    }
}
