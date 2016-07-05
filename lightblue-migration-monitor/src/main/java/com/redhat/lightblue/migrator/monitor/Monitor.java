package com.redhat.lightblue.migrator.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.http.LightblueHttpClient;

public abstract class Monitor {

    private final static Logger LOGGER = LoggerFactory.getLogger(Monitor.class);

    protected final MonitorConfiguration monitorCfg;
    protected final LightblueClient lightblueClient;

    public Monitor(MonitorConfiguration monitorCfg) {
        this.monitorCfg = monitorCfg;
        if (monitorCfg.getClientConfig() != null) {
            lightblueClient = new LightblueHttpClient(monitorCfg.getClientConfig());
        } else {
            lightblueClient = new LightblueHttpClient();
        }
    }

    public void runCheck(final Notifier... notifiers){
        try{
            doRunCheck(notifiers);
        } catch (LightblueException e) {
            onError(e, "Unable to communicate with lightblue", notifiers);
        } catch (Throwable e) {
            onError(e, notifiers);
        }
    }

    protected abstract void doRunCheck(final Notifier... notifiers) throws LightblueException;

    protected void onSuccess(final Notifier... notifiers) {
        for (Notifier n : notifiers) {
            n.sendSuccess();
        }
    }

    protected void onFailure(final String message, final Notifier... notifiers) {
        LOGGER.error("Check Failed: " + message);
        for (Notifier n : notifiers) {
            n.sendFailure(message);
        }
    }

    protected void onError(final String message, final Notifier... notifiers) {
        LOGGER.error("Check Errored: " + message);
        for (Notifier n : notifiers) {
            n.sendFailure(message);
        }
    }

    protected void onError(final Throwable throwable, final Notifier... notifiers) {
        onError(throwable, throwable.getMessage(), notifiers);
    }

    protected void onError(final Throwable throwable, final String message, final Notifier... notifiers) {
        LOGGER.error("Check Errored: " + message, throwable);
        for (Notifier n : notifiers) {
            n.sendError(message);
        }
    }

}
