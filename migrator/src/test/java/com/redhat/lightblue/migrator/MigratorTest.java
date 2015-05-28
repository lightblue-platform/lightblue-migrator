package com.redhat.lightblue.migrator;

import java.util.Map;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.redhat.lightblue.client.*;
import com.redhat.lightblue.client.request.*;
import com.redhat.lightblue.client.http.*;
import com.redhat.lightblue.client.request.data.*;
import com.redhat.lightblue.client.expression.query.*;
import com.redhat.lightblue.client.enums.*;
import com.redhat.lightblue.client.projection.*;


import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;


public class MigratorTest extends AbstractMigratorController {

    private String versionMigrationJob;
    private String versionMigrationConfiguration;
    private String versionSourceCustomer;
    private String versionDestinationCustomer;

    public MigratorTest() throws Exception {}
        
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

    @After
    public void clearData() throws Exception {
        LightblueClient cli = new LightblueHttpClient();
        DataDeleteRequest req=new DataDeleteRequest("activeExecution",null);
        req.where(new ValueQuery("objectType",ExpressionOperation.EQ,"activeExecution"));
        cli.data(req);
        req=new DataDeleteRequest("migrationJob",null);
        req.where(new ValueQuery("objectType",ExpressionOperation.EQ,"migrationJob"));
        cli.data(req);
        req=new DataDeleteRequest("migrationConfiguration",null);
        req.where(new ValueQuery("objectType",ExpressionOperation.EQ,"migrationConfiguration"));
        cli.data(req);
    }

    // @Test
    // public void controllerTest() throws Exception {
    //     loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-testmigrator.json");
    //     loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
        
    //     MainConfiguration cfg=new MainConfiguration();
    //     cfg.setName("continuum");
    //     cfg.setHostName("hostname");
    //     Controller controller=new Controller(cfg);

    //     Breakpoint.stop("Controller:start");
    //     Breakpoint.stop("Controller:loadconfig");
    //     Breakpoint.stop("Controller:createconfig");
        
    //     controller.start();

    //     Breakpoint.waitUntil("Controller:start");
    //     System.out.println("Resuming controller after startup");
    //     Breakpoint.resume("Controller:start");

    //     Breakpoint.waitUntil("Controller:loadconfig");
    //     System.out.println("Controller will load configurations");
    //     Breakpoint.resume("Controller:loadconfig");

    //     // Put breakpoints in migrator controller before we start one
    //     Breakpoint.stop("MigratorController:start");
    //     Breakpoint.stop("MigratorController:findandlock");
    //     Breakpoint.stop("MigratorController:process");
    //     Breakpoint.stop("MigratorController:unlock");
    //     Breakpoint.stop("MigratorController:end");
        
    //     Breakpoint.waitUntil("Controller:createconfig");
    //     System.out.println("Checking controllers");

    //     Map<String,Controller.MigrationProcess> prc=controller.getMigrationProcesses();
    //     Assert.assertEquals(1,prc.size());
        
    //     Breakpoint.resume("Controller:createconfig");

    //     // Controller created threads, now check the migrator controller thread progress

    //     Breakpoint.waitUntil("MigratorController:start");
    //     Breakpoint.resume("MigratorController:start");
    //     System.out.println("Migrator controller started");

    //     Breakpoint.waitUntil("MigratorController:findandlock");
    //     Breakpoint.resume("MigratorController:findandlock");

    //     Breakpoint.waitUntil("MigratorController:process");
    //     System.out.println("Processing");
    //     TestMigrator.count=0;
    //     Breakpoint.resume("MigratorController:process");

    //     Breakpoint.waitUntil("MigratorController:unlock");
    //     // At this point, there must be on TestMigrator instance running
    //     Assert.assertEquals(1,TestMigrator.count);

    //     controller.interrupt();
    // }

    @Test
    public void migrateTest() throws Exception {
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations.json");
        loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
        loadData("sourceCustomer", versionMigrationJob, "./test/data/load-source-customers.json");
       
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        // Stop when we retrieve source docs
        Breakpoint.stop("Migrator:sourceDocs");
        controller.start();

        Breakpoint.waitUntil("Migrator:sourceDocs");
        // We got source docs, peek
        Thread[] threads=new Thread[1];
        Assert.assertEquals(1,controller.
                            getMigrationProcesses().
                            get("customerMigration_0").mig.getMigratorThreads().enumerate(threads));
        
        Migrator m=(Migrator)threads[0];
        Assert.assertEquals(5,m.getSourceDocs().size());

        Breakpoint.stop("Migrator:destDocs");
        Breakpoint.resume("Migrator:sourceDocs");

        Breakpoint.waitUntil("Migrator:destDocs");
        Assert.assertEquals(0,m.getDestDocs().size());
                
        Breakpoint.stop("Migrator:insertDocs");
        Breakpoint.resume("Migrator:destDocs");

        Breakpoint.waitUntil("Migrator:insertDocs");
        Assert.assertEquals(5,m.getInsertDocs().size());

        Breakpoint.stop("Migrator:rewriteDocs");
        Breakpoint.resume("Migrator:insertDocs");

        Breakpoint.waitUntil("Migrator:rewriteDocs");
        Assert.assertEquals(0,m.getRewriteDocs().size());

        Breakpoint.stop("Migrator:complete");
        Breakpoint.resume("Migrator:rewriteDocs");

        Breakpoint.waitUntil("Migrator:complete");

        LightblueClient cli = new LightblueHttpClient();
        DataFindRequest req=new DataFindRequest("destCustomer","1.0.0");
        req.select(new FieldProjection("*",true,true));
        req.where(new ValueQuery("objectType",ExpressionOperation.EQ,"destCustomer"));
        req.sort(new SortCondition("_id",SortDirection.ASC));
        JsonNode[] ret=cli.data(req,JsonNode[].class);
        
        Breakpoint.resume("Migrator:complete");

        Assert.assertEquals(5,ret.length);
    }
         
}

