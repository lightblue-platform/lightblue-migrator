package com.redhat.lightblue.migrator;

import java.util.Properties;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

public class MainConfiguration {

    public static final Options options;
    
    private String name;
    private String hostName;
    private String clientConfig;

    static {
        options = new Options();
        
        options.addOption(OptionBuilder.
                          withArgName("name").
                          withLongOpt("name").
                          hasArg(true).
                          withDescription("Name of checker instance").
                          isRequired().
                          create('n'));
        options.addOption(OptionBuilder.
                          withArgName("hostname").
                          withLongOpt("hostname").
                          hasArg(true).
                          withDescription("Hostname running the checker instance").
                          isRequired().
                          create('h'));
        options.addOption(OptionBuilder.
                          withArgName("config").
                          withLongOpt("config").
                          hasArg(true).
                          withDescription("Path to configuration file for migration").
                          isRequired().
                          create('c'));
    }


    public String getName() {
        return name;
    }

    public void setName(String s) {
        name=s;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String s) {
        hostName=s;
    }

    public String getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(String s) {
        clientConfig=s;
    }

    
    public static Properties processArguments(String[] args){
        Properties prop=new Properties();        
        try {
            PosixParser parser = new PosixParser();
            CommandLine commandline = parser.parse(options, args);
            Option[] opts = commandline.getOptions();
            for (Option opt : opts) {
                prop.setProperty(opt.getLongOpt(), opt.getValue() == null ? "true" : opt.getValue());
            }
        } catch (ParseException e) {
            return null;
        }
        return prop;
    }

    public static MainConfiguration getCfg(Properties p) {
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName(p.getProperty("name"));
        cfg.setHostName(p.getProperty("hostname"));
        cfg.setClientConfig(p.getProperty("config"));
        return cfg;
    }
}
