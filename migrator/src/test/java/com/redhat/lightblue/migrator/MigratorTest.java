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

        Breakpoint.stop("Controller:start");
        Breakpoint.stop("Controller:loadconfig");
        Breakpoint.stop("Controller:createconfig");
        
        controller.start();

        Breakpoint.waitUntil("Controller:start");
        System.out.println("Resuming controller after startup");
        Breakpoint.resume("Controller:start");

        Breakpoint.waitUntil("Controller:loadconfig");
        System.out.println("Controller will load configurations");
        Breakpoint.resume("Controller:loadconfig");

        // Put breakpoints in migrator controller before we start one
        Breakpoint.stop("MigratorController:start");
        Breakpoint.stop("MigratorController:findandlock");
        Breakpoint.stop("MigratorController:process");
        Breakpoint.stop("MigratorController:unlock");
        Breakpoint.stop("MigratorController:end");
        
        Breakpoint.waitUntil("Controller:createconfig");
        System.out.println("Checking controllers");

        Map<String,Controller.MigrationProcess> prc=controller.getMigrationProcesses();
        Assert.assertEquals(1,prc.size());
        
        Breakpoint.resume("Controller:createconfig");

        // Controller created threads, now check the migrator controller thread progress

        Breakpoint.waitUntil("MigratorController:start");
        Breakpoint.resume("MigratorController:start");
        System.out.println("Migrator controller started");

        Breakpoint.waitUntil("MigratorController:findandlock");
        Breakpoint.resume("MigratorController:findandlock");

        Breakpoint.waitUntil("MigratorController:process");
        System.out.println("Processing");
        TestMigrator.count=0;
        Breakpoint.resume("MigratorController:process");

        Breakpoint.waitUntil("MigratorController:unlock");
        // At this point, there must be on TestMigrator instance running
        Assert.assertEquals(1,TestMigrator.count);

        controller.interrupt();
    }
         
}

