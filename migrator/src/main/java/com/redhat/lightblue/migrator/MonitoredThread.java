package com.redhat.lightblue.migrator;

/**
 * Interface for monitored threads
 */
public interface MonitoredThread {

    void registerThreadMonitor(ThreadMonitor monitor);
    void ping(String msg);
    
}
