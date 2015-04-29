package com.redhat.lightblue.migrator.features;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.togglz.core.Feature;
import org.togglz.core.manager.TogglzConfig;
import org.togglz.core.repository.StateRepository;
import org.togglz.core.repository.cache.CachingStateRepository;
import org.togglz.core.repository.jdbc.JDBCStateRepository;
import org.togglz.core.repository.util.DefaultMapSerializer;
import org.togglz.core.user.UserProvider;

@ApplicationScoped
public class LightblueMigrationConfiguration implements TogglzConfig {

    Logger logger = LoggerFactory.getLogger(LightblueMigrationConfiguration.class);

    private DataSource dataSource;

    // http://www.togglz.org/apidocs/2.1.0.Final/org/togglz/core/repository/jdbc/JDBCStateRepository.Builder.html#noCommit(boolean)
    private boolean noCommit = true;

    private String tableName;

    private int cacheSeconds = 180;

    public LightblueMigrationConfiguration(String jndiPathToConfigDatasource, String tableName) throws NamingException {
        init(jndiPathToConfigDatasource, tableName, 180, true);
    }

    public LightblueMigrationConfiguration(String jndiPathToConfigDatasource, String tableName, int cacheSeconds, boolean noCommit) throws NamingException {
        init(jndiPathToConfigDatasource, tableName, cacheSeconds, noCommit);
    }

    @Inject
    public LightblueMigrationConfiguration(@Named("lightblueMigrationConfigurationFileName") String configFileName) throws IOException, NamingException {
        InputStream in = getClass().getResourceAsStream(configFileName);

        Properties props = new Properties();
        props.load(in);

        String datasourceJndi = props.getProperty("datasourceJndi");
        String tableName = props.getProperty("tableName");
        int cacheSeconds = Integer.parseInt(props.getProperty("cacheSeconds", "180"));
        boolean noCommit = Boolean.parseBoolean(props.getProperty("noCommit", "true"));

        init(datasourceJndi, tableName, cacheSeconds, noCommit);
    }

    public void init(String jndiPathToConfigDatasource, String tableName, int cacheSeconds, boolean noCommit) throws NamingException {
        InitialContext ctx = new InitialContext();
        this.dataSource = (DataSource)ctx.lookup(jndiPathToConfigDatasource);
        this.tableName = tableName;
        this.cacheSeconds = cacheSeconds;
        this.noCommit = noCommit;

        logger.debug("Initialized LightblueMigrationConfiguration: jndiPathToConfigDatasource="+jndiPathToConfigDatasource+", tableName="+tableName+", cacheSeonds="+cacheSeconds+",noCommit="+noCommit);
    }

    @Override
    public Class<? extends Feature> getFeatureClass() {
        return LightblueMigrationFeatures.class;
    }

    @Override
    public StateRepository getStateRepository() {
        return new CachingStateRepository(new JDBCStateRepository(dataSource, tableName, true, DefaultMapSerializer.singleline(), noCommit), cacheSeconds, TimeUnit.SECONDS);
    }

    @Override
    public UserProvider getUserProvider() {
        return new TogglzRandomUserProvider();
    }

}
