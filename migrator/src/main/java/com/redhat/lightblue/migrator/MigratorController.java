package com.redhat.lightblue.migrator;

public class MigratorController implements Runnable {

    private final MigrationConfiguration migrationConfiguration;
    private final Controller controller;

    public MigratorController(MigrationConfiguration migrationConfiguration,
                              Controller controller) {
        this.migrationConfiguration=migrationConfiguration;
        this.controller=controller;
    }

    public MigrationConfiguration getMigrationConfiguration() {
        return migrationConfiguration;
    }

    @Override
    public void run() {
    }
}
