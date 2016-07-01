package com.redhat.lightblue.migrator.monitor;

/**
 * An implementation of {@link Notifier} that is intended for Nagios alerts.
 * <b>NOTE:</b> This notifier will hard exit the application in order to provide
 * proper exit statuses as required by NRPE.
 *
 * @author Dennis Crissman
 */
public class NagiosNotifier implements Notifier {

    /**
     * logs message with status code 1 - warn
     */
    @Override
    public void sendFailure(String message) {
        System.out.print("Warning: " + message);
        System.exit(1);
    }

    @Override
    public void sendSuccess() {
        System.out.print("OK");
        System.exit(0);
    }

    /**
     * logs message with status code 2 - critical
     */
    @Override
    public void sendError(String message) {
        System.out.print("Critical: " + message);
        System.exit(2);
    }

}
