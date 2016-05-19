package com.redhat.lightblue.migrator.monitor;

import java.util.Properties;

import org.apache.commons.cli.HelpFormatter;

public class Main {

    public static void main(String[] args) throws Exception {
        Properties p = MonitorConfiguration.processArguments(args);
        if (p == null) {
            printHelp();
            return;
        }
        MonitorConfiguration cfg = MonitorConfiguration.getCfg(System.getProperties());
        cfg.applyProperties(p);

        new Monitor(cfg).runCheck(new NagiosNotifier());
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Main.class.getSimpleName(), MonitorConfiguration.options, true);
    }

}
