package com.redhat.lightblue.migrator;

public interface ThreadStatusListener {
    /**
     * This is called after the status of the monitored thread is
     * changed by the monitor
     */
    void threadStatusChanged(MonitoredThread thread,
                             ThreadMonitor monitor);
}
