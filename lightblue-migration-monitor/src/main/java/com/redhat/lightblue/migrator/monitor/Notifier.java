package com.redhat.lightblue.migrator.monitor;

public interface Notifier {

    void sendFailure(String message);

    void sendSuccess();

}
