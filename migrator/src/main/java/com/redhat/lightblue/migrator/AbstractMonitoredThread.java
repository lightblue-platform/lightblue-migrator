package com.redhat.lightblue.migrator;

/**
 * Base class for all monitored threads. Once created, this thread
 * needs to be registered with a ThreadMonitor. The implementing
 * thread should call ping periodically to notify progress. If ping is
 * not called for a long time, the thread monitor will try to interrup
 * and terminate the thread. A terminated thread will throw a runtime
 * exception next time it calls ping.
 */
public abstract class AbstractMonitoredThread extends Thread implements MonitoredThread {

    private ThreadMonitor monitor;
    
    public AbstractMonitoredThread() {
    }

    public AbstractMonitoredThread(String name) {
        super(name);
    }

    public AbstractMonitoredThread(ThreadGroup group,String name) {
        super(group,name);
    }

    @Override
    public void registerThreadMonitor(ThreadMonitor monitor) {
        this.monitor=monitor;
        ping();
    }

    public void ping() {
        ping(null);
    }
    
    @Override
    public void ping(String msg) {
        if(monitor!=null)
            monitor.ping(msg);
    }

    @Override
    public void notifyEnd() {
        if(monitor!=null)
            monitor.endThread();
    }

    @Override
    public final void run() {
        try {
            monitoredRun();
        } finally {
            notifyEnd();
        }
    }

    protected abstract void monitoredRun();
}
