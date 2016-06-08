package com.redhat.lightblue.migrator.monitor.newMigrationPeriods;

import java.util.List;

public interface Notifier {

    void sendFailure(List<String> configurationsMissingJobs);

    void sendSuccess();

}
