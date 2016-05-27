package com.redhat.lightblue.migrator.monitor;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MonitorConfiguration {

    public static final String OPTION_LIGHTBLUE_CLIENT_PROPERTIES = "config";
    public static final String OPTION_PERIODS = "periods";

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
                .longOpt(OPTION_LIGHTBLUE_CLIENT_PROPERTIES)
                .argName(OPTION_LIGHTBLUE_CLIENT_PROPERTIES)
                .build());

        options.addOption(Option.builder("p")
                .type(Integer.class)
                .required(false)
                .hasArg(true)
                .desc("Number of periods back to include in search")
                .longOpt(OPTION_PERIODS)
                .argName(OPTION_PERIODS)
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

    public static MonitorConfiguration processArguments(String[] args) {
        Properties props = new Properties();
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine commandline = parser.parse(options, args);
            Option[] opts = commandline.getOptions();
            for (Option opt : opts) {
                props.setProperty(opt.getLongOpt(), opt.getValue());
            }
        } catch (ParseException e) {
            return null;
        }

        if (props.isEmpty()) {
            return null;
        }

        MonitorConfiguration cfg = new MonitorConfiguration();
        cfg.applyProperties(props);
        return cfg;
    }

    public void applyProperties(Properties p) {
        String s = p.getProperty(OPTION_LIGHTBLUE_CLIENT_PROPERTIES);
        if (s != null) {
            setClientConfig(s);
        }

        String periods = p.getProperty(OPTION_PERIODS);
        if(periods != null){
            setPeriods(Integer.parseInt(periods));
        }
    }

}
