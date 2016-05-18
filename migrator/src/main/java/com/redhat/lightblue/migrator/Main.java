package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.HelpFormatter;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

public class Main implements Daemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private DaemonContext context;
    private MainConfiguration cfg;
    private Controller mainController;

    public static void main(String[] args) throws Exception {
        Properties p = MainConfiguration.processArguments(args);
        if (p == null) {
            printHelpAndExit();
        }
        MainConfiguration cfg = MainConfiguration.getCfg(System.getProperties());
        cfg.applyProperties(p);
        LOGGER.debug("Config:{}", cfg);
        Thread t = new Controller(cfg);
        t.start();
        t.join();
    }

    private static void printHelpAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Main.class.getSimpleName(), MainConfiguration.options, true);
        System.exit(1);
    }

    @Override
    public void init(DaemonContext context) throws DaemonInitException, Exception {
        LOGGER.info("Initializing " + getClass().getSimpleName());
        this.context = context;
        cfg = MainConfiguration.getCfg(System.getProperties());
        Properties p = MainConfiguration.processArguments(context.getArguments());
        if (p != null) {
            cfg.applyProperties(p);
        }

        mainController = new Controller(cfg);
        mainController.setDaemon(true);
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting main controller");
        mainController.start();
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping main controller");
        mainController.interrupt();
    }

    @Override
    public void destroy() {
    }

}
