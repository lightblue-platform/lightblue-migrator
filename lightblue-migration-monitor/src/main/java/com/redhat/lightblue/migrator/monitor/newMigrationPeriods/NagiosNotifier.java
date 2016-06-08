package com.redhat.lightblue.migrator.monitor.newMigrationPeriods;

import java.util.List;

import org.apache.commons.lang.StringUtils;

public class NagiosNotifier implements Notifier {

    @Override
    public void sendFailure(List<String> configurationsMissingJobs) {
        System.out.print("Jobs not being created: " + StringUtils.join(configurationsMissingJobs, ","));
        System.exit(2);
    }

    @Override
    public void sendSuccess() {
        System.exit(0);
    }

}
