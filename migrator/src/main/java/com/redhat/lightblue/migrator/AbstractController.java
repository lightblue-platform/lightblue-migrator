package com.redhat.lightblue.migrator;

import java.util.Date;
import java.util.HashSet;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Locking;

public abstract class AbstractController extends Thread {

    public enum JobType {
        GENERATED, NONGENERATED, ANY;
    }

    public static final Random random = new Random(new Date().getTime());

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractController.class);

    protected MigrationConfiguration migrationConfiguration;
    protected final Controller controller;
    protected final LightblueClient lbClient;
    protected final Locking locking;
    protected final Class migratorClass;
    protected final ThreadGroup migratorThreads;
    protected final HashSet<String> myLocks=new HashSet<>();
    protected boolean stopped=false;

    public AbstractController(Controller controller,MigrationConfiguration migrationConfiguration,String threadGroupName) {
        this.migrationConfiguration=migrationConfiguration;
        this.controller=controller;
        lbClient=controller.getLightblueClient();
        locking=lbClient.getLocking("migration");
        if(migrationConfiguration.getMigratorClass()==null)
            migratorClass=DefaultMigrator.class;
        else {
            try {
                migratorClass = Class.forName(migrationConfiguration.getMigratorClass());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        migratorThreads = new ThreadGroup(threadGroupName);
    }

    public void setStopped() {
        stopped=true;
        interrupt();
    }

    public ThreadGroup getMigratorThreads() {
        return migratorThreads;
    }

    public Controller getController() {
        return controller;
    }

    public MigrationConfiguration getMigrationConfiguration() {
        return migrationConfiguration;
    }

    public MigrationConfiguration reloadMigrationConfiguration() throws Exception {
        try {
            LOGGER.debug("Reloading migration configuration {}", migrationConfiguration.get_id());
            return controller.loadMigrationConfiguration(migrationConfiguration.get_id());
        } catch (Exception e) {
            LOGGER.error("Cannot reload migration configuration:" + e);
            throw e;
        }
    }

    /**
     * Attempts to lock a migration job. If successful, return the migration job
     * and the active execution
     */
    public ActiveExecution lock(String id)
            throws Exception {
        LOGGER.debug("locking {}", id);
        if (!myLocks.contains(id)) {
            if (locking.acquire(id, null)) {
                myLocks.add(id);
                ActiveExecution ae = new ActiveExecution();
                ae.setMigrationJobId(id);
                ae.set_id(id);
                ae.setStartTime(new Date());
                return ae;
            }
        }
        return null;
    }

    public void unlock(String id) {
        LOGGER.debug("Unlocking {}", id);
        try {
            locking.release(id);
        } catch (Exception e) {
            LOGGER.error("Error unlocking {}", id, e);
        }
        myLocks.remove(id);
        Breakpoint.checkpoint("MigratorController:unlock");
    }

    public Migrator createMigrator(MigrationJob mj, ActiveExecution ae)
            throws Exception {
        Migrator migrator = (Migrator) migratorClass.getConstructor(ThreadGroup.class).newInstance(migratorThreads);
        migrator.setController(this);
        migrator.setMigrationJob(mj);
        migrator.setActiveExecution(ae);
        return migrator;
    }
}
