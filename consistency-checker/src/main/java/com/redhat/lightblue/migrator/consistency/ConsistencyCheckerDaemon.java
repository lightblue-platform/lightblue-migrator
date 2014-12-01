package com.redhat.lightblue.migrator.consistency;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonController;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsistencyCheckerDaemon implements Daemon{

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckerDaemon.class);

    private Thread consistencyCheckerThread;
    private ConsistencyChecker checker;

    public static void main(final String[] args) throws Exception {
        final ConsistencyCheckerDaemon daemon = new ConsistencyCheckerDaemon();
        daemon.init(new DaemonContext() {

            @Override
            public DaemonController getController() {
                return new ConsistencyCheckerDaemonController(daemon);
            }

            @Override
            public String[] getArguments() {
                return args;
            }
        });
        daemon.start();
    }

    @Override
    public void init(DaemonContext context) throws DaemonInitException, Exception {
        checker = ConsistencyCheckerCLI.buildConsistencyChecker(context.getArguments());
        consistencyCheckerThread = new Thread(checker, "ConsistencyChecker");
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting " + getClass().getName());
        consistencyCheckerThread.start();
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping " + getClass().getName());
        checker.setRun(false);
        consistencyCheckerThread.interrupt();
    }

    @Override
    public void destroy() {
        checker = null;
        consistencyCheckerThread = null;
    }

    private static class ConsistencyCheckerDaemonController implements DaemonController{

        private final Daemon daemon;

        public ConsistencyCheckerDaemonController(Daemon daemon){
            this.daemon = daemon;
        }

        @Override
        public void shutdown() throws IllegalStateException {
            try{
                daemon.stop();
                daemon.destroy();
            }
            catch(Exception e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reload() throws IllegalStateException {
            try{
                daemon.stop();
                daemon.start();
            }
            catch(Exception e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public void fail() throws IllegalStateException {
            shutdown();
        }

        @Override
        public void fail(String message) throws IllegalStateException {
            LOGGER.error(message);
            fail();
        }

        @Override
        public void fail(Exception exception) throws IllegalStateException {
            fail("Failed for unknown reason", exception);
        }

        @Override
        public void fail(String message, Exception exception)
                throws IllegalStateException {
            LOGGER.error(message, exception);
            fail();
        }

    }

}
