package com.redhat.lightblue.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMigrator extends Migrator {

    private static final Logger LOGGER=LoggerFactory.getLogger(DefaultMigrator.class);

    @Override
    public String migrate() {
        // Read from source, write to dest
        return null;
    }
}

