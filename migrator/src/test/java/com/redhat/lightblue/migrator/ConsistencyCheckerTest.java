package com.redhat.lightblue.migrator;

import java.util.Map;
import java.util.Date;

import java.text.SimpleDateFormat;

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


public class ConsistencyCheckerTest extends AbstractMigratorController {

    private String versionMigrationJob;
    private String versionMigrationConfiguration;
    private String versionSourceCustomer;
    private String versionDestinationCustomer;

    private final String DATE_FORMAT="yyyyMMdd'T'HH:mm:ss";
    private final SimpleDateFormat fmt=new SimpleDateFormat(DATE_FORMAT);
    
    public ConsistencyCheckerTest() throws Exception {}
        
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


    private Date date(String s) throws Exception {
        return fmt.parse("20150101T"+s);
    }
    

    @Test
    public void consistencyCheckerTest() throws Exception {
        clearData();
        Breakpoint.clearAll();
        LightblueClient cli = new LightblueHttpClient();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-cctest.json");

        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        // Setup data
        // First: modify the configuration to set the date to a known value: 20150101 00:00:00
        DataUpdateRequest upd=new DataUpdateRequest("migrationConfiguration",null);
        upd.where(Query.withValue("configurationName",Query.eq,"customers"));
        upd.updates(Update.set("timestampInitialValue",Literal.value(date("00:00:00"))));
        cli.data(upd);

        // Now is 00:01:00 of that day, so we expect to see 59 new migration jobs
        CCC.setNow(date("00:01:00"));

        // Period is 1 sec        
                    

        // Stop the migrator
        Breakpoint.stop("MigratorController:findandlock");

        // Stop when ccc starts
        Breakpoint.stop("CCC:start");
        controller.start();

        Breakpoint.waitUntil("CCC:start");
        System.out.println("Controller started");


        Breakpoint.stop("CCC:afterCreateJobs");
        Breakpoint.resume("CCC:start");

        // CCC is creating jobs here       

        Breakpoint.waitUntil("CCC:afterCreateJobs");
        // Put a breakpoint to the next time CCC wakes up and does stuff
        Breakpoint.stop("CCC:locked");

        // Jobs are created
        // Make sure they are what we expect
        DataFindRequest find=new DataFindRequest("migrationJob",null);
        find.where(Query.withValue("objectType",Query.eq,"migrationJob"));
        find.select(Projection.includeFieldRecursively("*"));
        MigrationJob[] jobs=cli.data(find,MigrationJob[].class);
        for(MigrationJob j:jobs)
            System.out.println(j.getQuery());
        Assert.assertEquals(58,jobs.length);

        CCC.setNow(date("00:01:01"));

        // Wait until CCC wakes up
        Breakpoint.resume("CCC:afterCreateJobs");
        System.out.println("Waiting until CCC locks");
        Breakpoint.waitUntil("CCC:locked");
        System.out.println("CCC Locked");
        Breakpoint.resume("CCC:locked");
        
        Breakpoint.stop("CCC:afterCreateJobs");
        Breakpoint.waitUntil("CCC:afterCreateJobs");
        // There should be one more now
        find=new DataFindRequest("migrationJob",null);
        find.where(Query.withValue("objectType",Query.eq,"migrationJob"));
        find.select(Projection.includeFieldRecursively("*"));
        jobs=cli.data(find,MigrationJob[].class);
        for(MigrationJob j:jobs)
            System.out.println(j.getQuery());
        Assert.assertEquals(59,jobs.length);


        // Stop everything
        Breakpoint.stop("Controller:end");
        controller.interrupt();
        Breakpoint.waitUntil("Controller:end");
        // Controller stopped, but wait a bit more to let others stop as well
        Thread.sleep(200);
    }

    @Test
    public void dupJobTest() throws Exception {
        clearData();
        Breakpoint.clearAll();
        LightblueClient cli = new LightblueHttpClient();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-cctest.json");

        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        // Setup data
        // First: modify the configuration to set the date to a known value: 20150101 00:00:00
        DataUpdateRequest upd=new DataUpdateRequest("migrationConfiguration",null);
        upd.where(Query.withValue("configurationName",Query.eq,"customers"));
        upd.updates(Update.set("timestampInitialValue",Literal.value(date("00:00:00"))));
        cli.data(upd);

        // Now is 00:01:00 of that day, so we expect to see 58 new migration jobs
        CCC.setNow(date("00:01:00"));

        // Period is 1 sec        
                    

        // Stop the migrator
        Breakpoint.stop("MigratorController:findandlock");

        // Stop when ccc starts
        Breakpoint.stop("CCC:start");
        controller.start();

        Breakpoint.waitUntil("CCC:start");
        System.out.println("Controller started");


        Breakpoint.stop("CCC:afterCreateJobs");
        Breakpoint.resume("CCC:start");

        // CCC is creating jobs here       

        Breakpoint.waitUntil("CCC:afterCreateJobs");
        // Put a breakpoint to the next time CCC wakes up and does stuff
        Breakpoint.stop("CCC:locked");

        // Jobs are created
        // Make sure they are what we expect
        DataFindRequest find=new DataFindRequest("migrationJob",null);
        find.where(Query.withValue("objectType",Query.eq,"migrationJob"));
        find.select(Projection.includeFieldRecursively("*"));
        MigrationJob[] jobs=cli.data(find,MigrationJob[].class);
        for(MigrationJob j:jobs)
            System.out.println(j.getQuery());
        Assert.assertEquals(58,jobs.length);

        // Reset the timestamp
        upd=new DataUpdateRequest("migrationConfiguration",null);
        upd.where(Query.withValue("configurationName",Query.eq,"customers"));
        upd.updates(Update.set("timestampInitialValue",Literal.value(date("00:00:00"))));
        cli.data(upd);

        
        CCC.setNow(date("00:01:01"));
        

        // Wait until CCC wakes up
        Breakpoint.resume("CCC:afterCreateJobs");
        System.out.println("Waiting until CCC locks");
        Breakpoint.waitUntil("CCC:locked");
        System.out.println("CCC Locked");
        Breakpoint.resume("CCC:locked");
        
        Breakpoint.stop("CCC:afterCreateJobs");
        Breakpoint.waitUntil("CCC:afterCreateJobs");
        // There should be one more now
        find=new DataFindRequest("migrationJob",null);
        find.where(Query.withValue("objectType",Query.eq,"migrationJob"));
        find.select(Projection.includeFieldRecursively("*"));
        jobs=cli.data(find,MigrationJob[].class);
        for(MigrationJob j:jobs)
            System.out.println(j.getQuery());
        Assert.assertEquals(59,jobs.length);


        // Stop everything
        Breakpoint.stop("Controller:end");
        controller.interrupt();
        Breakpoint.waitUntil("Controller:end");
        // Controller stopped, but wait a bit more to let others stop as well
        Thread.sleep(200);
    }

}
