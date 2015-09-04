package com.redhat.lightblue.migrator;

import java.io.IOException;

import java.util.UUID;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public abstract class AbstractController extends Thread {

    private static final Logger LOGGER=LoggerFactory.getLogger(AbstractController.class);

    private final String callerId=UUID.randomUUID().toString();
    
    protected MigrationConfiguration migrationConfiguration;
    protected final Controller controller;
    protected final LightblueClient lbClient;
    protected final Class migratorClass;
    protected final ThreadGroup migratorThreads;

    public AbstractController(Controller controller,MigrationConfiguration migrationConfiguration,String threadGroupName) {
        this.migrationConfiguration=migrationConfiguration;
        this.controller=controller;
        lbClient=controller.getLightblueClient();
        if(migrationConfiguration.getMigratorClass()==null)
            migratorClass=DefaultMigrator.class;
        else
            try {
                migratorClass=Class.forName(migrationConfiguration.getMigratorClass());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        
        migratorThreads=new ThreadGroup(threadGroupName);
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

    public MigrationConfiguration reloadMigrationConfiguration() {
        try {
            LOGGER.debug("Reloading migration configuration {}",migrationConfiguration.get_id());
            return controller.loadMigrationConfiguration(migrationConfiguration.get_id());
        } catch (Exception e) {
            LOGGER.error("Cannot reload migration configuration:"+e);
        }
        return null;
    }

    /**
     * Attempts to lock a migration job. If successful, return the migration job and the active execution
     */
    public ActiveExecution lock(String id)
        throws Exception {
        if(lbClient.getLocking("migration").acquire(callerId,id,null)) {
            ActiveExecution ae=new ActiveExecution();
            ae.setMigrationJobId(id);
            ae.setStartTime(new Date());
            return ae;
        } 
        return null;
    }

    public void unlock(String id) {
        LOGGER.debug("Unlocking {}",id);
        try {
            lbClient.getLocking("migration").release(callerId,id);
        } catch (Exception e) {}
        Breakpoint.checkpoint("MigratorController:unlock");
    }

    public Migrator createMigrator(MigrationJob mj,ActiveExecution ae)
        throws Exception {
        Migrator migrator=(Migrator)migratorClass.getConstructor(ThreadGroup.class).newInstance(migratorThreads);
        migrator.setController(this);
        migrator.setMigrationJob(mj);
        migrator.setActiveExecution(ae);
        return migrator;
    }
}
