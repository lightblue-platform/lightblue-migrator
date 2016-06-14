package com.redhat.lightblue.migrator.monitor;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.http.LightblueHttpClient;

public abstract class Monitor {

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

    public abstract void runCheck(final Notifier... notifiers) throws LightblueException;

}
