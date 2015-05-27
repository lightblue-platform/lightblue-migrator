package com.redhat.lightblue.migrator;

public class TestMigrator extends Migrator {

    public static int count;
    
    public String migrate() {
        count++;
        return null;
    }
}

