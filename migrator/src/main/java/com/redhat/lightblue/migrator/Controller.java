package com.redhat.lightblue.migrator;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.enums.ExpressionOperation;
import com.redhat.lightblue.client.response.LightblueException;
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
        public final AbstractController ccc;

        public MigrationProcess(MigrationConfiguration cfg,
                                MigratorController mig,
                                AbstractController ccc) {
            this.cfg=cfg;
            this.mig=mig;
            this.ccc=ccc;
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
        throws IOException, LightblueException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration",null);
        findRequest.where(withValue("consistencyCheckerName = " + cfg.getName()));
        findRequest.select(includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration:{}",findRequest.getBody());
        return lightblueClient.data(findRequest, MigrationConfiguration[].class);
    }

    /**
     * Load migration configuration based on its id
     */
    public MigrationConfiguration loadMigrationConfiguration(String migrationConfigurationId) 
        throws IOException, LightblueException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration",null);
        findRequest.where(withValue("_id",ExpressionOperation.EQ,migrationConfigurationId));
        findRequest.select(includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration");
        return lightblueClient.data(findRequest, MigrationConfiguration.class);
    }
    
    public LightblueClient getLightblueClient() {
        LightblueHttpClient httpClient;
        LOGGER.debug("Getting client, config={}",cfg.getClientConfig());
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
    public void createControllers(MigrationConfiguration[] configurations) throws Exception {
        for(MigrationConfiguration cfg:configurations) {
            if(!migrationMap.containsKey(cfg.get_id())) {
                LOGGER.debug("Creating a controller thread for configuration {}: {}",cfg.get_id(),cfg.getConfigurationName());
                MigratorController c=new MigratorController(this,cfg);
                AbstractController ccc;
                if(cfg.getTimestampFieldName()!=null&&
                   cfg.getTimestampFieldName().length()>0) {
                    if(cfg.getConsistencyCheckerControllerClass()!=null&&
                       cfg.getConsistencyCheckerControllerClass().length()>0)
                        ccc=(AbstractController)Class.forName(cfg.getConsistencyCheckerControllerClass()).
                            getConstructor(Controller.class,MigrationConfiguration.class).newInstance(this,cfg);
                    else
                        ccc=new ConsistencyCheckerController(this,cfg);
                } else
                    ccc=null;
                migrationMap.put(cfg.get_id(),new MigrationProcess(cfg,c,ccc));
                c.start();
                if(ccc!=null)
                    ccc.start();
            }
        }
    }

    @Override
    public void run() {
        LOGGER.debug("Starting controller");
        boolean interrupted=false;
        Breakpoint.checkpoint("Controller:start");
        while(!interrupted) {
            interrupted=isInterrupted();
            if(!interrupted) {
                try {
                    Breakpoint.checkpoint("Controller:loadconfig");
                    MigrationConfiguration[] cfg=getMigrationConfigurations();
                    createControllers(cfg);
                    Breakpoint.checkpoint("Controller:createconfig");
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
        for(MigrationProcess p:migrationMap.values()) {
            p.mig.interrupt();
            if(p.ccc!=null)
                p.ccc.interrupt();
        }
        Breakpoint.checkpoint("Controller:end");
    }
}
