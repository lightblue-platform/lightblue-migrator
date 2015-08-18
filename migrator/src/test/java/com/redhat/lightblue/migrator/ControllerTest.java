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
import com.redhat.lightblue.client.response.*;
import com.redhat.lightblue.client.http.*;
import com.redhat.lightblue.client.request.data.*;
import com.redhat.lightblue.client.expression.query.*;
import com.redhat.lightblue.client.enums.*;
import com.redhat.lightblue.client.projection.*;


import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

public class ControllerTest extends AbstractMigratorController {

    private String versionMigrationJob;
    private String versionMigrationConfiguration;
    private String versionSourceCustomer;
    private String versionDestinationCustomer;

    public ControllerTest() throws Exception {}
        
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

    @Test
    public void controllerTest() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-testmigrator.json");
        loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
        
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
        FakeMigrator.count=0;
        Breakpoint.resume("MigratorController:process");

        Breakpoint.waitUntil("MigratorController:unlock");
        // At this point, there must be one TestMigrator instance running
        Assert.assertEquals(1,FakeMigrator.count);
        Breakpoint.resume("MigratorController:unlock");

        controller.getMigrationProcesses().get("customerMigration_0").mig.interrupt();
        controller.interrupt();
        Thread.sleep(100);
    }
}

