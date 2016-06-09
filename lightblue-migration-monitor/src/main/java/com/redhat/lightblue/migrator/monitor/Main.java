package com.redhat.lightblue.migrator.monitor;

import org.apache.commons.cli.HelpFormatter;

import com.redhat.lightblue.migrator.monitor.HIR.HIRMonitor;
import com.redhat.lightblue.migrator.monitor.NMP.NMPMonitor;

public class Main {

    public static void main(String[] args) throws Exception {
        MonitorConfiguration cfg = MonitorConfiguration.processArguments(args);
        if (cfg == null) {
            printHelp();
            return;
        }

        cfg.applyProperties(System.getProperties());

        switch (cfg.getType()) {
            case NEW_MIGRATION_PERIODS:
                new NMPMonitor(cfg).runCheck(new NagiosNotifier());
                break;
            case HIGH_INCONSISTENCY_RATE:
                new HIRMonitor(cfg).runCheck(new NagiosNotifier());
                break;
        }

    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Main.class.getSimpleName(), MonitorConfiguration.options, true);
    }

}
