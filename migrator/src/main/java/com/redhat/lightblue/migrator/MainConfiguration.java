package com.redhat.lightblue.migrator;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MainConfiguration {

    public static final Options options;

    private String name;
    private String hostName;
    private String clientConfig;
    private Long threadTimeout;

    static {
        options = new Options();

        options.addOption(Option.builder("n")
                .argName("name")
                .longOpt("name")
                .hasArg(true)
                .desc("Name of checker instance")
                .required(true)
                .build());
        options.addOption(Option.builder("h")
                .argName("hostname")
                .longOpt("hostname")
                .hasArg(true)
                .desc("Hostname running the checker instance")
                .required(true)
                .build());
        options.addOption(Option.builder("c")
                .argName("config")
                .longOpt("config")
                .hasArg(true)
                .desc("Path to configuration file for migration")
                .required(true)
                .build());
        options.addOption(Option.builder("t")
                .argName("threadTimeout")
                .longOpt("threadTimeout")
                .hasArg(true)
                .desc("Maximum time thread is allowed to run (msecs)")
                .required(false)
                .build());
    }

    public String getName() {
        return name;
    }

    public void setName(String s) {
        name = s;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String s) {
        hostName = s;
    }

    public String getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(String s) {
        clientConfig = s;
    }

    public void setThreadTimeout(String s) {
        if (s != null) {
            setThreadTimeout(Long.valueOf(s));
        }
    }

    public void setThreadTimeout(Long l) {
        threadTimeout = l;
    }

    public Long getThreadTimeout() {
        return threadTimeout;
    }

    public String toString() {
        return "name=" + name + " hostName=" + hostName + " config=" + clientConfig;
    }

    public static Properties processArguments(String[] args) {
        Properties prop = new Properties();
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine commandline = parser.parse(options, args);
            Option[] opts = commandline.getOptions();
            for (Option opt : opts) {
                prop.setProperty(opt.getLongOpt(), opt.getValue() == null ? "true" : opt.getValue());
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
        return prop;
    }

    public static MainConfiguration getCfg(Properties p) {
        MainConfiguration cfg = new MainConfiguration();
        cfg.applyProperties(p);
        return cfg;
    }

    public void applyProperties(Properties p) {
        String s = p.getProperty("name");
        if (s != null) {
            setName(s);
        }
        s = p.getProperty("hostname");
        if (s != null) {
            setHostName(s);
        }
        s = p.getProperty("config");
        if (s != null) {
            setClientConfig(s);
        }
        s = p.getProperty("threadTimeout");
        if (s != null) {
            setThreadTimeout(s);
        }
    }
}
