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

    public void clearData() throws Exception {
        LightblueClient cli = new LightblueHttpClient();
        DataDeleteRequest req=new DataDeleteRequest("activeExecution",null);
        req.where(Query.withValue("objectType",Query.eq,"activeExecution"));
        cli.data(req);
        req=new DataDeleteRequest("migrationJob",null);
        req.where(Query.withValue("objectType",Query.eq,"migrationJob"));
        cli.data(req);
        req=new DataDeleteRequest("migrationConfiguration",null);
        req.where(Query.withValue("objectType",Query.eq,"migrationConfiguration"));
        cli.data(req);
    }


    @Test
    public void migrateTest() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations.json");
        loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
        loadData("sourceCustomer", versionSourceCustomer, "./test/data/load-source-customers.json");
       
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

        Breakpoint.stop("Migrator:complete");
        Breakpoint.resume("Migrator:sourceDocs");

        System.out.println("Waiting for completion");
        Breakpoint.waitUntil("Migrator:complete");

        LightblueClient cli = new LightblueHttpClient();
        DataFindRequest req=new DataFindRequest("destCustomer","1.0.0");
        req.select(Projection.includeFieldRecursively("*"));
        req.where(Query.withValue("objectType",Query.eq,"destCustomer"));
        req.sort(Sort.asc("_id"));
        JsonNode[] ret=cli.data(req,JsonNode[].class);
        
        System.out.println("Complete");
        Breakpoint.resume("Migrator:complete");

        Assert.assertEquals(5,ret.length);
        
        System.out.println("Interrupt controller");
        System.out.println("Interrupting "+controller.getMigrationProcesses().get("customerMigration_0").mig.getName());
        controller.getMigrationProcesses().get("customerMigration_0").mig.interrupt();
        controller.interrupt();
        System.out.println("Test ends");
        Thread.sleep(1000);
    }
         
}

