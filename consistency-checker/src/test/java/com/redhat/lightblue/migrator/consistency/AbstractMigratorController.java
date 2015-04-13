package com.redhat.lightblue.migrator.consistency;

import org.junit.BeforeClass;

import com.redhat.lightblue.test.utils.AbstractCRUDControllerWithRest;

public abstract class AbstractMigratorController extends AbstractCRUDControllerWithRest {

    @BeforeClass
    public static void prepareMetadataDatasources() {
        System.setProperty("mongo.datasource", "mongodata");
    }

    public AbstractMigratorController() throws Exception {
        super();
    }

}
