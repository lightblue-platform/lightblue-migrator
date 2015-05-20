package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.PropertyConfigurator;

import org.apache.commons.cli.HelpFormatter;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

public class Main implements Daemon {

    private static final Logger LOGGER=LoggerFactory.getLogger(Main.class);

    private DaemonContext context;
    private MainConfiguration cfg;
    private Controller mainController;
    
    public static void main(String[] args) throws Exception {
        Properties p=MainConfiguration.processArguments(args);
        if(p==null) {
            printHelpAndExit();
        }
        MainConfiguration cfg=MainConfiguration.getCfg(p);
        setupLogging(cfg);

        Thread t=new Controller(cfg);
        t.start();
        t.join();
    }


    private static void setupLogging(MainConfiguration c) {
        if (c.getLog4jConfig() != null && !c.getLog4jConfig().isEmpty()) {
            // watch for log4j changes using default delay (60 seconds)
            PropertyConfigurator.configureAndWatch(c.getLog4jConfig());
        }
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
        cfg=MainConfiguration.getCfg(System.getProperties());
        setupLogging(cfg);

        mainController=new Controller(cfg);
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
