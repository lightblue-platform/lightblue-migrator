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
import org.togglz.core.repository.StateRepository;
import org.togglz.core.repository.cache.CachingStateRepository;
import org.togglz.core.repository.jdbc.JDBCStateRepository;
import org.togglz.core.repository.util.DefaultMapSerializer;

@ApplicationScoped
public class LightblueMigrationStateRepositoryProvider {

    Logger logger = LoggerFactory.getLogger(LightblueMigrationStateRepositoryProvider.class);

    private final String dataSourceJndi, tableName;
    private final int cacheSeconds;
    private final boolean noCommit;

    public LightblueMigrationStateRepositoryProvider(String dataSourceJndi, String tableName, int cacheSeconds, boolean noCommit) {
        super();
        this.dataSourceJndi = dataSourceJndi;
        this.tableName = tableName;
        this.cacheSeconds = cacheSeconds;
        this.noCommit = noCommit;

        logger.debug("Initialing LightblueMigrationStateRepositoryProvider: dataSourceJndi="+dataSourceJndi+", tableName="+tableName+", cacheSeonds="+cacheSeconds+",noCommit="+noCommit);
    }

    @Inject
    public LightblueMigrationStateRepositoryProvider(@Named("lightblueMigrationConfigurationFileName") String configFileName) throws IOException {
        logger.debug("Initialing LightblueMigrationStateRepositoryProvider from property file: "+configFileName);
        Properties props = new Properties();
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("./"+configFileName)) {
            props.load(in);
        }

        this.dataSourceJndi = props.getProperty("datasourceJndi");
        this.tableName = props.getProperty("tableName");
        this.cacheSeconds = Integer.parseInt(props.getProperty("cacheSeconds", "180"));
        this.noCommit = Boolean.parseBoolean(props.getProperty("noCommit", "true"));

        logger.debug("Initialing LightblueMigrationStateRepositoryProvider: dataSourceJndi="+dataSourceJndi+", tableName="+tableName+", cacheSeonds="+cacheSeconds+",noCommit="+noCommit);
    }

    public StateRepository getStateRepository() throws NamingException {

        InitialContext c = new InitialContext();
        DataSource dataSource = (DataSource) c.lookup(dataSourceJndi);

        JDBCStateRepository jdbcStateRepository = new JDBCStateRepository(dataSource, tableName, true, DefaultMapSerializer.singleline(), noCommit);

        if (cacheSeconds >= 0)
            return new CachingStateRepository(jdbcStateRepository, cacheSeconds, TimeUnit.SECONDS);
        else
            return jdbcStateRepository;
    }

    public StateRepository getStateRepository(DataSource dataSource) {

        JDBCStateRepository jdbcStateRepository = new JDBCStateRepository(dataSource, tableName, true, DefaultMapSerializer.singleline(), noCommit);

        if (cacheSeconds >= 0)
            return new CachingStateRepository(jdbcStateRepository, cacheSeconds, TimeUnit.SECONDS);
        else
            return jdbcStateRepository;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isNoCommit() {
        return noCommit;
    }

    public String getDataSourceJndi() {
        return dataSourceJndi;
    }

    public int getCacheSeconds() {
        return cacheSeconds;
    }

}
