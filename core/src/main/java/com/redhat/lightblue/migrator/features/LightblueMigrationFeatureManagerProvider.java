package com.redhat.lightblue.migrator.features;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.togglz.core.manager.FeatureManager;
import org.togglz.core.manager.FeatureManagerBuilder;
import org.togglz.core.spi.FeatureManagerProvider;

public class LightblueMigrationFeatureManagerProvider implements FeatureManagerProvider {

    protected Logger logger = LoggerFactory.getLogger(LightblueMigrationFeatureManagerProvider.class);

    protected static FeatureManager featureManager;

    protected String configFileName;

    public LightblueMigrationFeatureManagerProvider(String configFileName) {
        this.configFileName = configFileName;
    }

    @Override
    public int priority() {
        return 0;
    }

    private LightblueMigrationConfiguration createConfig() throws IOException, NamingException {
        InputStream in = getClass().getResourceAsStream(configFileName);

        Properties props = new Properties();
        props.load(in);

        String datasourceJndi = props.getProperty("datasourceJndi");
        String tableName = props.getProperty("tableName");
        int cacheSeconds = Integer.parseInt(props.getProperty("cacheSeconds", "180"));
        boolean noCommit = Boolean.parseBoolean(props.getProperty("noCommit", "true"));

        return new LightblueMigrationConfiguration(datasourceJndi, tableName, cacheSeconds, noCommit);
    }

    @Override
    public FeatureManager getFeatureManager() {
        if (featureManager == null) {
            try {
                featureManager = new FeatureManagerBuilder()
                .togglzConfig(createConfig())
                .build();

            } catch (NamingException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        return featureManager;

    }

}
