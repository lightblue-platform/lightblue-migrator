package com.redhat.lightblue.migrator;

public class ConsistencyCheckerController implements Runnable {

    private final MigrationConfiguration migrationConfiguration;
    private final Controller controller;

    public ConsistencyCheckerController(MigrationConfiguration migrationConfiguration,
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
