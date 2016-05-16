package com.redhat.lightblue.migrator;

import java.util.*;

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
        req=new DataDeleteRequest("sourceCustomer","1.0.0");
        req.where(Query.withValue("objectType",Query.eq,"sourceCustomer"));
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
        controller.getMigrationProcesses().get("customerMigration_0").mig.setStopped();
        controller.setStopped();
        System.out.println("Test ends");
        Thread.sleep(1000);
    }

    @Test
    public void threadMonitoringTest_interrupt() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-interruptiblemigrator.json");
        loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
        loadData("sourceCustomer", versionSourceCustomer, "./test/data/load-source-customers.json");
        
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        // 10msec thread timeout
        cfg.setThreadTimeout(10l);
        Controller controller=new Controller(cfg);
        // We need this here to make sure controller has a chance to
        // initialize the threads before we start waiting for them
        Breakpoint.stop("Controller:createconfig");
        Breakpoint.stop("Migrator:interrupted");
        // This will put a breakpoint to the test class
        Breakpoint.stop("Migrator:getSourceDocuments");
        controller.start();
        Breakpoint.waitUntil("Controller:createconfig");

        Breakpoint.resume("Controller:createconfig");

        Breakpoint.waitUntil("Migrator:getSourceDocuments");
        Breakpoint.resume("Migrator:getSourceDocuments");

        Breakpoint.stop("ThreadMonitor:check");
        // Wait for 100 msec, the thread monitor should interrupt the thread
        try {
            Thread.sleep(100);
        } catch (Exception e) {}
        controller.getThreadMonitor().runNow();
        System.out.println("Waiting until check wakes up");
        Breakpoint.waitUntil("ThreadMonitor:check");
        Breakpoint.resume("ThreadMonitor:check");
        System.out.println("Thread monitor woke up");
        Breakpoint.waitUntil("Migrator:interrupted");
        Breakpoint.resume("Migrator:interrupted");
        Assert.assertTrue(InterruptibleStuckMigrator.numInterrupted>0);
   }

    
    @Test
    public void threadMonitoringTest_abandon() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-uninterruptiblemigrator.json");
        loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
        loadData("sourceCustomer", versionSourceCustomer, "./test/data/load-source-customers.json");
        
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        // 10msec thread timeout
        cfg.setThreadTimeout(10l);
        Controller controller=new Controller(cfg);
        // We need this here to make sure controller has a chance to
        // initialize the threads before we start waiting for them
        Breakpoint.stop("Controller:createconfig");
        Breakpoint.stop("Migrator:interrupted");
        // This will put a breakpoint to the test class
        Breakpoint.stop("Migrator:getSourceDocuments");
        controller.start();
        Breakpoint.waitUntil("Controller:createconfig");

        Breakpoint.resume("Controller:createconfig");

        Breakpoint.waitUntil("Migrator:getSourceDocuments");
        Breakpoint.resume("Migrator:getSourceDocuments");

        Breakpoint.stop("ThreadMonitor:check");
        // Wait for 100 msec, the thread monitor should interrupt the thread
        try {
            Thread.sleep(100);
        } catch (Exception e) {}
        controller.getThreadMonitor().runNow();
        System.out.println("Waiting until check wakes up");
        Breakpoint.waitUntil("ThreadMonitor:check");
        Breakpoint.resume("ThreadMonitor:check");
        System.out.println("Thread monitor woke up");
        try {
            Thread.sleep(100);
        } catch (Exception e) {}
        controller.getThreadMonitor().runNow();
        try {
            Thread.sleep(100);
        } catch (Exception e) {}
        // Thread must be abandoned by now
        Assert.assertEquals(1,controller.getThreadMonitor().getThreadCount(ThreadMonitor.Status.abandoned));
    }
    
    @Test
    public void cleanupOldJobs() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-uninterruptiblemigrator.json");
        
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        LightblueClient cli=controller.getLightblueClient();

        Date now=new Date();
        Date ago=new Date(System.currentTimeMillis()-30l*24l*60l*60l*1000l);
        
        List<MigrationJob> jobs=new ArrayList<>();
        MigrationJob j=new MigrationJob();

        j.set_id("completed_not_generated_old");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("completed");
        j.setGenerated(false);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        MigrationJob.JobExecution je=new MigrationJob.JobExecution();
        je.setStatus("completed");
        je.setOwnerName("name");
        je.setHostName("host");
        je.setActualStartDate(ago);
        je.setActualEndDate(ago);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("completed_generated_old");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("completed");
        j.setGenerated(true);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setStatus("completed");
        je.setOwnerName("name");
        je.setHostName("host");
        je.setActualStartDate(ago);
        je.setActualEndDate(ago);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("failed_not_generated_old");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("failed");
        j.setGenerated(false);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setStatus("failed");
        je.setOwnerName("name");
        je.setHostName("host");
        je.setActualStartDate(now);
        je.setActualEndDate(now);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("failed_generated_old");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("failed");
        j.setGenerated(true);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setStatus("failed");
        je.setOwnerName("name");
        je.setHostName("host");
        je.setActualStartDate(now);
        je.setActualEndDate(now);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("completed_not_generated_new");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("completed");
        j.setGenerated(false);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setOwnerName("name");
        je.setStatus("completed");
        je.setHostName("host");
        je.setActualStartDate(now);
        je.setActualEndDate(now);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("completed_generated_new");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("completed");
        j.setGenerated(true);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setOwnerName("name");
        je.setStatus("completed");
        je.setHostName("host");
        je.setActualStartDate(now);
        je.setActualEndDate(now);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("available_generated_new");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("available");
        j.setGenerated(true);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setOwnerName("name");
        je.setStatus("available");
        je.setHostName("host");
        je.setActualStartDate(now);
        je.setActualEndDate(now);
        j.getJobExecutions().add(je);
        jobs.add(j);

        DataInsertRequest req=new DataInsertRequest("migrationJob",null);
        req.create(jobs);
        cli.data(req);

        DataFindRequest f=new DataFindRequest("migrationJob",null);
        f.select(Projection.includeField("*"));
        MigrationJob[] all=cli.data(f,MigrationJob[].class);
        
        CleanupThread t=new CleanupThread(controller);
        t.cleanupOldJobs(cli,new Date(System.currentTimeMillis()-60000l));
        
        f=new DataFindRequest("migrationJob",null);
        f.select(Projection.includeField("*"));
        MigrationJob[] cleanedup=cli.data(f,MigrationJob[].class);
        Assert.assertTrue(cleanedup.length<all.length&&all.length>0);
        for(MigrationJob job:cleanedup) {
            Assert.assertNotEquals("completed_generated_old",job.get_id());
        }
        for(MigrationJob l:jobs) {
            if(!l.get_id().equals("completed_generated_old")) {
                boolean found=false;
                for(MigrationJob k:cleanedup) {
                    if(k.get_id().equals(l.get_id())) {
                        found=true;
                        break;
                    }
                }
                Assert.assertTrue(found);
            }
        }
    }

    @Test
    public void enableStuckJobs() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-uninterruptiblemigrator.json");
        
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        LightblueClient cli=controller.getLightblueClient();

        Date now=new Date();
        Date ago=new Date(System.currentTimeMillis()-5l*60l*60l*1000l);
        
        List<MigrationJob> jobs=new ArrayList<>();
        MigrationJob j=new MigrationJob();

        j.set_id("completed_not_generated_old");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("completed");
        j.setGenerated(false);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        MigrationJob.JobExecution je=new MigrationJob.JobExecution();
        je.setStatus("completed");
        je.setOwnerName("name");
        je.setHostName("host");
        je.setActualStartDate(ago);
        je.setActualEndDate(ago);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("active_old");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("active");
        j.setGenerated(true);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setStatus("active");
        je.setOwnerName("name");
        je.setHostName("host");
        je.setActualStartDate(ago);
        je.setActualEndDate(ago);
        j.getJobExecutions().add(je);
        jobs.add(j);

        j=new MigrationJob();
        j.set_id("active_new");
        j.setConfigurationName("c");
        j.setScheduledDate(new Date());
        j.setStatus("active");
        j.setGenerated(false);
        j.setJobExecutions(new ArrayList<MigrationJob.JobExecution>());
        je=new MigrationJob.JobExecution();
        je.setStatus("active");
        je.setOwnerName("name");
        je.setHostName("host");
        je.setActualStartDate(now);
        je.setActualEndDate(now);
        j.getJobExecutions().add(je);
        jobs.add(j);


        DataInsertRequest req=new DataInsertRequest("migrationJob",null);
        req.create(jobs);
        cli.data(req);

        DataFindRequest f=new DataFindRequest("migrationJob",null);
        f.select(Projection.includeField("*"));
        MigrationJob[] all=cli.data(f,MigrationJob[].class);
        
        CleanupThread t=new CleanupThread(controller);
        t.enableStuckJobs(cli,new Date(System.currentTimeMillis()-60000l));
        
        f=new DataFindRequest("migrationJob",null);
        f.select(Projection.includeField("*"));
        MigrationJob[] enabled=cli.data(f,MigrationJob[].class);
        Assert.assertEquals(all.length,enabled.length);

        for(MigrationJob job:enabled) {
            if(job.get_id().equals("active_old"))
                Assert.assertEquals("available",job.getStatus());
        }
        
    }
    
    @Test
    public void healthcheckTest() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-cctest.json");
        
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        Breakpoint.stop("Controller:createconfig");
        Breakpoint.stop("CCC:start");
        controller.start();

        Breakpoint.waitUntil("Controller:createconfig");
        // Here all threads are created and running
        Breakpoint.waitUntil("CCC:start");
        Breakpoint.resume("CCC:start");
        Assert.assertTrue(controller.getMigrationProcesses().get("customerMigration_0").ccc.isAlive());
        // Kill the ccc thread
        Breakpoint.stop("CCC:end");
        controller.getMigrationProcesses().get("customerMigration_0").ccc.setStopped();
        Breakpoint.waitUntil("CCC:end");
        Thread.sleep(150);
        Assert.assertFalse(controller.getMigrationProcesses().get("customerMigration_0").ccc.isAlive());
        controller.healthcheck(controller.getMigrationProcesses().get("customerMigration_0").cfg);
        Assert.assertTrue(controller.getMigrationProcesses().get("customerMigration_0").ccc.isAlive());
    }
}

