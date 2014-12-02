package com.redhat.lightblue.migrator.consistency;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.daemon.support.DaemonLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsistencyCheckerDaemon implements Daemon{

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckerDaemon.class);

    private Thread consistencyCheckerThread;
    private Timer isThreadAliveWatcher;

    public static void main(final String[] args) throws Exception {
        DaemonLoader.load(ConsistencyCheckerDaemon.class.getCanonicalName(), args);
        DaemonLoader.start();
    }

    @Override
    public void init(DaemonContext context) throws DaemonInitException, Exception {
        ConsistencyChecker checker = ConsistencyCheckerCLI.buildConsistencyChecker(context.getArguments());
        consistencyCheckerThread = new Thread(checker, "ConsistencyCheckerRunner");
        isThreadAliveWatcher = new Timer("ConsistencyCheckerStatusMonitor");
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting " + getClass().getName());
        consistencyCheckerThread.start();
        
        /*
         * Timer insures that if the underlying ConsistencyChecker stopped for any reason,
         * that the service will be shutdown also. This really shouldn't happen,
         * so if it does, it is considered an error.
         */
        isThreadAliveWatcher.schedule(new TimerTask() {
            @Override
            public void run() {
                if(!consistencyCheckerThread.isInterrupted() && !consistencyCheckerThread.isAlive()){
                    LOGGER.error(ConsistencyChecker.class.getName() + " has stopped running, killing service.");
                    DaemonLoader.stop();
                    DaemonLoader.destroy();
                    System.exit(1);
                }
            }
        }, 5000);
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping " + getClass().getName());
        isThreadAliveWatcher.cancel();
        if((consistencyCheckerThread != null) && consistencyCheckerThread.isAlive()){
            consistencyCheckerThread.interrupt();
        }
    }

    @Override
    public void destroy() {
        consistencyCheckerThread = null;
        isThreadAliveWatcher = null;
    }

}
