package com.redhat.lightblue.migrator;

public abstract class Migrator extends Runnable {

    private MigratorController controller;
    private MigrationJob migrationJob;
    private ActiveExecution activeExecution;

    public void setController(MigratorController c) {
        this.controller=c;
    }

    public void setMigrationJob(MigrationJob m) {
        migrationJob=m;
    }

    public void setActiveExecution(ActiveExecution e) {
        activeExecution=e;
    }
    
    @Override
    public final run() {
        
    }
}

