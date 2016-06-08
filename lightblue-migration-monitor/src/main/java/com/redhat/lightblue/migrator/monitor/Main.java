package com.redhat.lightblue.migrator.monitor;

import org.apache.commons.cli.HelpFormatter;

import com.redhat.lightblue.migrator.monitor.newMigrationPeriods.Monitor;
import com.redhat.lightblue.migrator.monitor.newMigrationPeriods.NagiosNotifier;

public class Main {

    public static void main(String[] args) throws Exception {
        MonitorConfiguration cfg = MonitorConfiguration.processArguments(args);
        if (cfg == null) {
            printHelp();
            return;
        }

        cfg.applyProperties(System.getProperties());

        new Monitor(cfg).runCheck(new NagiosNotifier());
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Main.class.getSimpleName(), MonitorConfiguration.options, true);
    }

}
