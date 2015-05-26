package com.redhat.lightblue.migrator;

import java.util.Map;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;


public class MigratorTest extends AbstractMigratorController {

    private String versionMigrationJob;
    private String versionMigrationConfiguration;
    private String versionSourceCustomer;
    private String versionDestinationCustomer;

    private static boolean initialized;

    //    public MigratorTest() throws Exception {}
    
    //    @Before
    public MigratorTest() throws Exception {
        if(!initialized) {
            loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations.json");
            loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
            loadData("sourceCustomer", versionSourceCustomer, "./test/data/load-source-customers.json");
            initialized=true;
        }
    }
    
    @Override
    protected JsonNode[] getMetadataJsonNodes() throws Exception {
        ObjectNode jsonActiveExecution = (ObjectNode) loadJsonNode("./activeExecution.json");
        ObjectNode jsonMigrationJob = (ObjectNode) loadJsonNode("./migrationJob.json");
        ObjectNode jsonMigrationConfiguration = (ObjectNode) loadJsonNode("./migrationConfiguration.json");
        ObjectNode jsonSourceCustomer = (ObjectNode) loadJsonNode("./test/metadata/source-customer.json");
        ObjectNode jsonDestinationCustomer = (ObjectNode) loadJsonNode("./test/metadata/destination-customer.json");

        versionMigrationJob = parseEntityVersion(jsonMigrationJob);
        versionMigrationConfiguration = parseEntityVersion(jsonMigrationConfiguration);
        versionSourceCustomer = parseEntityVersion(jsonSourceCustomer);
        versionDestinationCustomer = parseEntityVersion(jsonDestinationCustomer);

        return new JsonNode[]{
                removeHooks(grantAnyoneAccess(jsonMigrationJob)),
                removeHooks(grantAnyoneAccess(jsonMigrationConfiguration)),
                removeHooks(grantAnyoneAccess(jsonActiveExecution)),
                jsonSourceCustomer,
                jsonDestinationCustomer
        };
    }

    @Test
    public void controllerTest() throws Exception {
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        controller.startbp.stop();
        controller.loadconfigbp.stop();
        controller.createconfigbp.stop();
        
        controller.start();

        controller.startbp.waitUntil();
        System.out.println("Resuming controller after startup");
        controller.startbp.resume();

        controller.loadconfigbp.waitUntil();
        System.out.println("Controller will load configurations");
        controller.loadconfigbp.resume();

        controller.createconfigbp.waitUntil();
        System.out.println("Checking controllers");

        Map<String,Controller.MigrationProcess> prc=controller.getMigrationProcesses();
        Assert.assertEquals(1,prc.size());
        
        controller.createconfigbp.resume();

        controller.interrupt();
    }
         
}

