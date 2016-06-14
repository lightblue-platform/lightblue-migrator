package com.redhat.lightblue.migrator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;

/**
 * This is the main thread. It runs until it is interrupted. Periodically it
 * reads migration configurations, and creates MigratorController and
 * ConsistencyChecker threads. All those threads are derived from
 * AbstractController, and have their own thread groups.
 */
public class Controller extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);

    private final MainConfiguration cfg;
    private final LightblueClient lightblueClient;
    private final Map<String, MigrationProcess> migrationMap = new HashMap<>();
    private final ThreadMonitor threadMonitor;
    private volatile boolean stopped=false;

    public static class MigrationProcess {
        public MigrationConfiguration cfg;
        public MigratorController mig;
        public AbstractController ccc;

        public MigrationProcess(MigrationConfiguration cfg,
                                MigratorController mig,
                                AbstractController ccc) {
            this.cfg = cfg;
            this.mig = mig;
            this.ccc = ccc;
        }
    }

    public Controller(MainConfiguration cfg) {
        this.cfg = cfg;
        lightblueClient = getLightblueClient();
        Long tt = cfg.getThreadTimeout();
        if (tt == null) {
            threadMonitor = new ThreadMonitor();
        } else {
            threadMonitor = new ThreadMonitor(tt);
        }
        threadMonitor.start();
    }

    public void setStopped() {
        this.stopped=true;
        interrupt();
    }

    public Map<String,MigrationProcess> getMigrationProcesses() {
        return migrationMap;
    }

    public MainConfiguration getMainConfiguration() {
        return cfg;
    }

    public ThreadMonitor getThreadMonitor() {
        return threadMonitor;
    }

    /**
     * Read configurations from the database whose name matches this instance
     * name
     */
    public MigrationConfiguration[] getMigrationConfigurations()
            throws IOException, LightblueException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration", null);
        findRequest.where(Query.withValue("consistencyCheckerName", Query.eq, cfg.getName()));
        findRequest.select(Projection.includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration:{}", findRequest.getBody());
        return lightblueClient.data(findRequest, MigrationConfiguration[].class);
    }

    /**
     * Read a configuration from the database whose name matches the the given
     * configuration name
     */
    public MigrationConfiguration getMigrationConfiguration(String configurationName)
            throws IOException, LightblueException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration", null);
        findRequest.where(Query.and(
                Query.withValue("configurationName", Query.eq, configurationName),
                Query.withValue("consistencyCheckerName", Query.eq, cfg.getName()))
        );
        findRequest.select(Projection.includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration:{}", findRequest.getBody());
        return lightblueClient.data(findRequest, MigrationConfiguration.class);
    }

    /**
     * Load migration configuration based on its id
     */
    public MigrationConfiguration loadMigrationConfiguration(String migrationConfigurationId)
            throws IOException, LightblueException {
        DataFindRequest findRequest = new DataFindRequest("migrationConfiguration", null);
        findRequest.where(Query.withValue("_id", Query.eq, migrationConfigurationId));
        findRequest.select(Projection.includeFieldRecursively("*"));
        LOGGER.debug("Loading configuration");
        return lightblueClient.data(findRequest, MigrationConfiguration.class);
    }

    public LightblueClient getLightblueClient() {
        LightblueHttpClient httpClient;
        LOGGER.debug("Getting client, config={}", cfg.getClientConfig());
        if (cfg.getClientConfig() != null) {
            return new LightblueHttpClient(cfg.getClientConfig());
        } else {
            return new LightblueHttpClient();
        }
    }

    private boolean shouldHaveConsistencyChecker(MigrationConfiguration cfg) {
        return cfg.getPeriod() != null && cfg.getPeriod().trim().length() > 0;
    }

    private AbstractController getConsistencyCheckerController(MigrationConfiguration cfg) {
        AbstractController ccc = null;
        try {
            if (shouldHaveConsistencyChecker(cfg)) {
                if (cfg.getConsistencyCheckerControllerClass() != null
                        && cfg.getConsistencyCheckerControllerClass().length() > 0) {
                    ccc = (AbstractController) Class.forName(cfg.getConsistencyCheckerControllerClass()).
                            getConstructor(Controller.class, MigrationConfiguration.class).newInstance(this, cfg);
                } else {
                    ccc = new ConsistencyCheckerController(this, cfg);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Cannot create consistency checker controller for {}:{}", cfg.getConfigurationName(), e);
        }
        return ccc;
    }

    /**
     * Creates controller threads for migrators and consistency checkers based
     * on the configuration loaded from the db.
     *
     * For each configuration item, a migrator controller thread is created and
     * started.
     *
     * Once created, each thread manages its own lifecycle. If the corresponding
     * configuration is removed, thread terminates, or it is modified, thread
     * behaves accordingly.
     */
    public void createControllers(MigrationConfiguration[] configurations) throws Exception {
        for (MigrationConfiguration cfg : configurations) {
            MigrationProcess process = migrationMap.get(cfg.get_id());
            if (process == null) {
                LOGGER.debug("Creating a controller thread for configuration {}: {}", cfg.get_id(), cfg.getConfigurationName());
                MigratorController c = new MigratorController(this, cfg);
                if (c instanceof MonitoredThread) {
                    ((MonitoredThread) c).registerThreadMonitor(threadMonitor);
                }
                AbstractController ccc = getConsistencyCheckerController(cfg);;
                if (ccc instanceof MonitoredThread) {
                    ((MonitoredThread) ccc).registerThreadMonitor(threadMonitor);
                }
                migrationMap.put(cfg.get_id(), new MigrationProcess(cfg, c, ccc));
                c.start();
                if (ccc != null) {
                    ccc.start();
                }
            } else {
                healthcheck(cfg);
            }
        }
    }

    public void healthcheck(MigrationConfiguration cfg) {
        // Healthcheck
        MigrationProcess process = migrationMap.get(cfg.get_id());
        if (process != null) {
            process.cfg = cfg;
            if (!process.mig.isAlive()) {
                LOGGER.error("Migrator thread for {} is not alive, recreating", cfg.getConfigurationName());
                process.mig = new MigratorController(this, cfg);
                if (process.mig instanceof MonitoredThread) {
                    ((MonitoredThread) process.mig).registerThreadMonitor(threadMonitor);
                }
                process.mig.start();
            }
            if (shouldHaveConsistencyChecker(cfg)) {
                if (process.ccc == null || (process.ccc != null && !process.ccc.isAlive())) {
                    LOGGER.error("Consistency checker for {} is not alive, recreating", cfg.getConfigurationName());
                    process.ccc = getConsistencyCheckerController(cfg);
                    if (process.ccc != null) {
                        if (process.ccc instanceof MonitoredThread) {
                            ((MonitoredThread) process.ccc).registerThreadMonitor(threadMonitor);
                        }
                        process.ccc.start();
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        LOGGER.debug("Starting controller");        
        Breakpoint.checkpoint("Controller:start");
        CleanupThread cleanup = new CleanupThread(this);
        if (cfg.getThreadTimeout() != null) {
            cleanup.setPeriod(cfg.getThreadTimeout() * 4);
        }
        cleanup.start();
        while(!stopped) {
            try {
                Breakpoint.checkpoint("Controller:loadconfig");
                MigrationConfiguration[] cfg=getMigrationConfigurations();
                createControllers(cfg);
                Breakpoint.checkpoint("Controller:createconfig");
                Thread.sleep(30000);
            } catch (Throwable e) {
                LOGGER.error("Error during configuration load:"+e);
            }
        }
        for(MigrationProcess p:migrationMap.values()) {
            p.mig.setStopped();
            if(p.ccc!=null) {
                p.ccc.setStopped();
            }
        }
        Breakpoint.checkpoint("Controller:end");
        cleanup.interrupt();
    }
}
