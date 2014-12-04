package com.redhat.lightblue.migrator.consistency;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.daemon.support.DaemonLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsistencyCheckerDaemon implements Daemon{

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckerDaemon.class);

    private DaemonContext context;
    private Thread consistencyCheckerThread = null;

    private Timer isThreadAliveWatcher = null;

    public static void main(final String[] args) throws Exception {
        DaemonLoader.load(ConsistencyCheckerDaemon.class.getName(), args);
        DaemonLoader.start();
    }

    @Override
    public void init(DaemonContext context) throws DaemonInitException, Exception {
        LOGGER.info("Initializing " + getClass().getSimpleName());

        this.context = context;

        consistencyCheckerThread = createConsistencyCheckerThread();
    }

    @Override
    public synchronized void start() throws Exception {
        LOGGER.info("Starting " + getClass().getSimpleName());

        consistencyCheckerThread.start();

        if(isThreadAliveWatcher == null){
            isThreadAliveWatcher = new Timer("WorkerMonitor");
            isThreadAliveWatcher.scheduleAtFixedRate(new IsAliveTimerTask(-1), 5000, 5000);
        }
    }

    @Override
    public synchronized void stop() throws Exception {
        LOGGER.info("Stopping " + getClass().getSimpleName());

        isThreadAliveWatcher.cancel();

        if(consistencyCheckerThread != null){
            consistencyCheckerThread.interrupt();
        }
    }

    @Override
    public void destroy() {
        LOGGER.info("Destroying " + getClass().getSimpleName());

        consistencyCheckerThread = null;
        isThreadAliveWatcher = null;
    }

    private Thread createConsistencyCheckerThread(){
        return new Thread(
                ConsistencyCheckerCLI.buildConsistencyChecker(context.getArguments()),
                "ConsistencyCheckerRunner");
    }

    /**
     * Ensures that if the ConsistencyChecker Tread goes down for any reason
     * that the Daemon handles it appropriately by either attempting a restart
     * or shutting down.
     */
    private class IsAliveTimerTask extends TimerTask{

        private final int maxRetries;
        private int retriesAttempted = 0;

        public IsAliveTimerTask(int maxRetries){
            this.maxRetries = maxRetries;
        }

        @Override
        public synchronized void run() {
            if(!consistencyCheckerThread.isInterrupted()
                    && !consistencyCheckerThread.isAlive()){

                if(maxRetries < 0){
                    retry();
                }
                else if(retriesAttempted >= maxRetries){
                    shutdownDaemon();
                }
                else{
                    retry();
                    retriesAttempted ++;
                }
            }
            else{
                retriesAttempted = 0;
            }
        }

        private void retry(){
            LOGGER.warn(ConsistencyChecker.class.getSimpleName() + " has stopped running, attempting restart.");
            consistencyCheckerThread = createConsistencyCheckerThread();
            System.gc();
            DaemonLoader.start();
        }

        private void shutdownDaemon(){
            LOGGER.error(ConsistencyChecker.class.getSimpleName() + " has stopped running, killing service.");
            DaemonLoader.stop();
            DaemonLoader.destroy();
            System.exit(1);
        }

    }

}
