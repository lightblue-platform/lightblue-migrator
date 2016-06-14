package com.redhat.lightblue.migrator.monitor;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MonitorConfiguration {

    public static final String OPTION_LIGHTBLUE_CLIENT_PROPERTIES = "config";
    public static final String OPTION_JOB = "job";
    public static final String OPTION_PERIODS = "periods";
    public static final String OPTION_CONFIGURATION_NAME = "configurationName";
    public static final String OPTION_THRESHOLD = "threshold";

    public static final Options options;

    private String clientConfig;
    private JobType type;
    private Integer periods;
    private String configurationName;
    private Integer threshold;

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

        options.addOption(Option.builder("j")
                .type(JobType.class)
                .required(true)
                .hasArg(true)
                .desc("Job that should be executed")
                .longOpt(OPTION_JOB)
                .argName(OPTION_JOB)
                .build());

        //New migration period options
        options.addOption(Option.builder("p")
                .type(Integer.class)
                .required(false)
                .hasArg(true)
                .desc("Number of periods back to include in search")
                .longOpt(OPTION_PERIODS)
                .argName(OPTION_PERIODS)
                .build());

        //High inconsistency rate options
        options.addOption(Option.builder("cn")
                .type(String.class)
                .required(false)
                .hasArg(true)
                .desc("configurationName to interogate")
                .longOpt(OPTION_CONFIGURATION_NAME)
                .argName(OPTION_CONFIGURATION_NAME)
                .build());

        options.addOption(Option.builder("t")
                .type(Integer.class)
                .required(false)
                .hasArg(true)
                .desc("Inclusive threshold to use in determining if an alert needs to be generated")
                .longOpt(OPTION_THRESHOLD)
                .argName(OPTION_THRESHOLD)
                .build());
    }

    public String getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(String clientConfig) {
        this.clientConfig = clientConfig;
    }

    public JobType getType() {
        return type;
    }

    public void setType(JobType type) {
        this.type = type;
    }

    public Integer getPeriods(){
        return periods;
    }

    public void setPeriods(Integer periods){
        this.periods = periods;
    }

    public String getConfigurationName() {
        return configurationName;
    }

    public void setConfigurationName(String configurationName) {
        this.configurationName = configurationName;
    }

    public Integer getThreshold() {
        return threshold;
    }

    public void setThreshold(Integer threshold) {
        this.threshold = threshold;
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

        String type = p.getProperty(OPTION_JOB);
        if (type != null) {
            setType(JobType.valueOf(type));
        }

        String periods = p.getProperty(OPTION_PERIODS);
        if(periods != null){
            setPeriods(Integer.parseInt(periods));
        }

        String configurationName = p.getProperty(OPTION_CONFIGURATION_NAME);
        if (configurationName != null) {
            setConfigurationName(configurationName);
        }

        String threshold = p.getProperty(OPTION_THRESHOLD);
        if (threshold != null) {
            setThreshold(Integer.parseInt(threshold));
        }
    }

}
