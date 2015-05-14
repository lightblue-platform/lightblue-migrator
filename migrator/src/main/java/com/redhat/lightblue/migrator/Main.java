package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.PropertyConfigurator;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
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
    private Thread consistencyCheckerThread = null;

    public static void main(String[] args) throws Exception {
        processArguments(args);

        ConsistencyChecker checker = buildConsistencyChecker();
        checker.run();
    }

    public static ConsistencyChecker buildConsistencyChecker() {
        ConsistencyChecker checker = new ConsistencyChecker();
        checker.setConsistencyCheckerName(System.getProperty("name"));
        checker.setHostName(System.getProperty("hostname"));
        checker.setConfigPath(System.getProperty("config"));
        checker.setMigrationConfigurationEntityVersion(System.getProperty("configversion"));
        checker.setMigrationJobEntityVersion(System.getProperty("jobversion"));
        checker.setSourceConfigPath(System.getProperty("sourceconfig"));
        checker.setDestinationConfigPath(System.getProperty("destinationconfig"));
        return checker;
    }
    
    /**
     * Read all configurations from the database
     */
    public static MigrationConfiguration[] getMigrationConfiguration(LightblueClient client,String cfgVersion, String name)
        throws IOException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration",cfgVersion);
        findRequest.where(withValue("consistencyCheckerName = " + name));
        findRequest.select(includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration");
        return client.data(findRequest, MigrationConfiguration[].class);
    }

    private static LightblueClient getLightblueClient(String configPath) {
        LightblueHttpClient httpClient;
        if (configPath != null) {
            httpClient = new LightblueHttpClient(configPath);
        } else {
            httpClient = new LightblueHttpClient();
        }
        return new LightblueHystrixClient(httpClient, "migrator", "primaryClient");
    }


    /**
     * Process command line, add all command line options to System properties
     */
    public static void processArguments(String[] args){
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("name").withLongOpt("name").hasArg(true).withDescription("Name of checker instance").isRequired().create('n'));
        options.addOption(OptionBuilder.withArgName("hostname").withLongOpt("hostname").hasArg(true).withDescription("Hostname running the checker instance").isRequired().create('h'));
        options.addOption(OptionBuilder.withArgName("config").withLongOpt("config").hasArg(true).withDescription("Path to configuration file for migration").isRequired().create('c'));
        options.addOption(OptionBuilder.withArgName("configversion").withLongOpt("configversion").hasArg(true).withDescription("migrationConfiguration Entity Version").isRequired().create('v'));
        options.addOption(OptionBuilder.withArgName("jobversion").withLongOpt("jobversion").hasArg(true).withDescription("migrationJob Entity Version").isRequired().create('j'));
        options.addOption(OptionBuilder.withArgName("sourceconfig").withLongOpt("sourceconfig").hasArg(true).withDescription("Path to configuration file for source").isRequired().create('s'));
        options.addOption(OptionBuilder.withArgName("destinationconfig").withLongOpt("destinationconfig").hasArg(true).withDescription("Path to configuration file for destination").isRequired().create('d'));
        
        String log4jConfig = System.getProperty("log4j.configuration");
        if (log4jConfig != null && !log4jConfig.isEmpty()) {
            // watch for log4j changes using default delay (60 seconds)
            PropertyConfigurator.configureAndWatch(log4jConfig);
        }

        try {
            PosixParser parser = new PosixParser();
            CommandLine commandline = parser.parse(options, args);
            Option[] opts = commandline.getOptions();
            for (Option opt : opts) {
                System.setProperty(opt.getLongOpt(), opt.getValue() == null ? "true" : opt.getValue());
            }
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(Main.class.getSimpleName(), options, true);
            System.out.println("\n");
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }


    @Override
    public void init(DaemonContext context) throws DaemonInitException, Exception {
        LOGGER.info("Initializing " + getClass().getSimpleName());
        this.context = context;
        consistencyCheckerThread = createConsistencyCheckerThread();
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting " + getClass().getSimpleName());
        consistencyCheckerThread.start();
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping " + getClass().getSimpleName());
        if(consistencyCheckerThread != null){
            consistencyCheckerThread.interrupt();
        }
    }

    @Override
    public void destroy() {
        LOGGER.info("Destroying " + getClass().getSimpleName());
        consistencyCheckerThread = null;
    }

    private Thread createConsistencyCheckerThread(){
        processArguments(context.getArguments());
        return new Thread(buildConsistencyChecker(),"ConsistencyCheckerRunner");
    }

}
