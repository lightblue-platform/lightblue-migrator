package com.redhat.lightblue.migrator;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;
import com.redhat.lightblue.client.expression.query.ValueQuery;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

public class Controller extends Thread {

    private static final Logger LOGGER=LoggerFactory.getLogger(Controller.class);
    
    private final MainConfiguration cfg;
    private final LightblueClient lightblueClient;
    private final Map<String,MigrationProcess> migrationMap=new HashMap<>();


    public static class MigrationProcess {
        public final MigrationConfiguration cfg;
        public final MigratorController mig;

        public MigrationProcess(MigrationConfiguration cfg,
                                MigratorController mig) {
            this.cfg=cfg;
            this.mig=mig;
        }
    }
    
    public Controller(MainConfiguration cfg) {
        this.cfg=cfg;
        this.lightblueClient=getLightblueClient();
    }

    public Map<String,MigrationProcess> getMigrationProcesses() {
        return migrationMap;
    }

    public MainConfiguration getMainConfiguration() {
        return cfg;
    }
    
    /**
     * Read configurations from the database whose name matches this instance name
     */
    public MigrationConfiguration[] getMigrationConfigurations()
        throws IOException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration",null);
        findRequest.where(withValue("consistencyCheckerName = " + cfg.getName()));
        findRequest.select(includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration:{}",findRequest.getBody());
        return lightblueClient.data(findRequest, MigrationConfiguration[].class);
    }
    
    public LightblueClient getLightblueClient() {
        LightblueHttpClient httpClient;
        if (cfg.getClientConfig() != null) {
            httpClient = new LightblueHttpClient(cfg.getClientConfig());
        } else {
            httpClient = new LightblueHttpClient();
        }
        return new LightblueHystrixClient(httpClient, "migrator", "primaryClient");
    }

    /**
     * Creates controller threads for migrators and consistency
     * checkers based on the configuration loaded from the db.
     *
     * For each configuration item, a migrator controller thread is
     * created and started.
     *
     * Once created, each thread manages its own lifecycle. If the
     * corresponding configuration is removed, thread terminates, or
     * it is modified, thread behaves accordingly.
     */
    public void createControllers(MigrationConfiguration[] configurations) throws IOException {
        for(MigrationConfiguration cfg:configurations) {
            if(!migrationMap.containsKey(cfg.get_id())) {
                LOGGER.debug("Creating a controller thread for configuration {}: {}",cfg.get_id(),cfg.getConfigurationName());
                MigratorController c=new MigratorController(this,cfg);
                migrationMap.put(cfg.get_id(),new MigrationProcess(cfg,c));
                c.start();
            }
        }
    }

    @Override
    public void run() {
        LOGGER.debug("Starting controller");
        boolean interrupted=false;
        Breakpoint.checkpoint("Controller:start");
        while(!interrupted) {
            interrupted=Thread.isInterrupted();
            if(!interrupted) {
                try {
                    Breakpoint.checkpoint("Controller:loadconfig");
                    MigrationConfiguration[] cfg=getMigrationConfigurations();
                    createControllers(cfg);
                    Breakpoint.checkpoint("Controller:createconfig");
                } catch (InterruptedException ie) {
                    interrupted=true;
                } catch (Exception e) {
                    LOGGER.error("Error during configuration load:"+e);
                }
            }
            if(!interrupted) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    interrupted=true;
                }
            }
        }
    }
}
