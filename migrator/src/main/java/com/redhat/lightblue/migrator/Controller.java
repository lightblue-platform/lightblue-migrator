package com.redhat.lightblue.migrator;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

public class Controller {

    private static final Logger LOGGER=LoggerFactory.getLogger(Controller.class);
    
    // Name of the daemon instance
    private final String instanceName;
    private final String configPath;
    private final String cfgVersion;

    private final LightblueClient lightblueClient;

    private ThreadGroup migratorControllers;
    private ThreadGroup consistencyCheckerControllers;
    
    public Controller(String instanceName,
                      String configPath,
                      String cfgVersion) {
        this.instanceName=instanceName;
        this.configPath=configPath;
        this.cfgVersion=cfgVersion;

        this.lightblueClient=getLightblueClient();
    }

    public LightblueClient getLightblueClient() {
        LightblueHttpClient httpClient;
        if (configPath != null) {
            httpClient = new LightblueHttpClient(configPath);
        } else {
            httpClient = new LightblueHttpClient();
        }
        return new LightblueHystrixClient(httpClient, "migrator", "primaryClient");
    }

    public MigrationConfiguration[] getConfiguration()
        throws IOException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration",cfgVersion);
        findRequest.where(withValue("consistencyCheckerName = " + instanceName));
        findRequest.select(includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration");
        return lightblueClient.data(findRequest, MigrationConfiguration[].class);
    }

    public ThreadGroup getMigratorControllers() {
        return migratorControllers;
    }

    public ThreadGroup getConsistencyCheckerControllers() {
        return consistencyCheckerControllers;
    }
    
    /**
     * Creates controller threads for migrators and consistency
     * checkers based on the configuration loaded from the db
     *
     * For each configuration item, a migrator controller thread and a
     * consistency checker controller thread is created. Threads are
     * not started.
     */
    public void createControllers() throws IOException {
        if(migratorControllers==null&&consistencyCheckerControllers==null) {
            MigrationConfiguration[] cfg=getConfiguration();
            if(cfg==null||cfg.length==0) {
                LOGGER.error("No configurations for {}",instanceName);
                // Let it terminate
            } else {
                LOGGER.debug("Configuration loaded, {} items",cfg.length);
                migratorControllers=new ThreadGroup("MigratorControllers");
                consistencyCheckerControllers=new ThreadGroup("ConsistencyCheckers");
                
                for(MigrationConfiguration configItem:cfg) {
                    LOGGER.debug("Creating a migration controller thread for {}",configItem.getConfigurationName());
                    new Thread(new MigratorController(configItem,this),"migratorController:"+configItem.getConfigurationName());
                    LOGGER.debug("Creating a consistency checker controller thread for {}",configItem.getConfigurationName());
                    new Thread(new ConsistencyCheckerController(configItem,this),"ConsistencyCheckerController:"+configItem.getConfigurationName());
                }
            }
        } else {
            throw new IllegalStateException("Controller threads are already created");
        }
    }
}
