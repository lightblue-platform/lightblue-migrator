package com.redhat.lightblue.migrator.monitor;

public class NagiosNotifier implements Notifier {

    @Override
    public void sendFailure(String message) {
        System.out.print(message);
        System.exit(2);
    }

    @Override
    public void sendSuccess() {
        System.exit(0);
    }

}
