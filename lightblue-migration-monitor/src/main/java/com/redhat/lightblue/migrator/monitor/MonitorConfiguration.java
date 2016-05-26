package com.redhat.lightblue.migrator.monitor;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MonitorConfiguration {

    public static final Options options;

    private String clientConfig;
    private Integer periods;

    static {
        options = new Options();

        options.addOption(Option.builder("c")
                .type(String.class)
                .required(true)
                .hasArg(true)
                .desc("Path to configuration file for migration")
                .longOpt("config")
                .argName("config")
                .build());

        options.addOption(Option.builder("p")
                .type(Integer.class)
                .required(false)
                .hasArg(true)
                .desc("Number of periods back to include in search")
                .longOpt("periods")
                .argName("periods")
                .build());
    }

    public String getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(String clientConfig) {
        this.clientConfig = clientConfig;
    }

    public Integer getPeriods(){
        return periods;
    }

    public void setPeriods(Integer periods){
        this.periods = periods;
    }

    public static Properties processArguments(String[] args) {
        Properties prop = new Properties();
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine commandline = parser.parse(options, args);
            Option[] opts = commandline.getOptions();
            for (Option opt : opts) {
                prop.setProperty(opt.getLongOpt(),
                        opt.getValue() == null ? "true" : opt.getValue());
            }
        } catch (ParseException e) {
            return null;
        }
        return prop;
    }

    public static MonitorConfiguration getCfg(Properties p) {
        MonitorConfiguration cfg = new MonitorConfiguration();
        cfg.applyProperties(p);
        return cfg;
    }

    public void applyProperties(Properties p) {
        String s = p.getProperty("config");
        if (s != null) {
            setClientConfig(s);
        }

        Integer periods = Integer.getInteger(p.getProperty("periods"));
        if(periods != null){
            setPeriods(periods);
        }
    }

}
