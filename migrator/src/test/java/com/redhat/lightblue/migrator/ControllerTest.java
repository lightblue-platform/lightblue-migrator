package com.redhat.lightblue.migrator;

import java.util.Map;

import org.junit.Test;
import org.junit.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;

import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;

public class ControllerTest extends AbstractMigratorController {

    private String versionMigrationJob;
    private String versionMigrationConfiguration;
    private String versionSourceCustomer;
    private String versionDestinationCustomer;

    public ControllerTest() throws Exception {
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

    public void clearData() throws Exception {
        LightblueClient cli = new LightblueHttpClient();
        DataDeleteRequest req = new DataDeleteRequest("activeExecution", null);
        req.where(Query.withValue("objectType", Query.BinOp.eq, "activeExecution"));
        cli.data(req);
        req = new DataDeleteRequest("migrationJob", null);
        req.where(Query.withValue("objectType", Query.BinOp.eq, "migrationJob"));
        cli.data(req);
        req = new DataDeleteRequest("migrationConfiguration", null);
        req.where(Query.withValue("objectType", Query.BinOp.eq, "migrationConfiguration"));
        cli.data(req);
    }

    @Test
    public void controllerTest() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-testmigrator.json");
        loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");

        MainConfiguration cfg = new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller = new Controller(cfg);

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

        Map<String, Controller.MigrationProcess> prc = controller.getMigrationProcesses();
        Assert.assertEquals(1, prc.size());

        Breakpoint.resume("Controller:createconfig");

        // Controller created threads, now check the migrator controller thread progress
        Breakpoint.waitUntil("MigratorController:start");
        Breakpoint.resume("MigratorController:start");
        System.out.println("Migrator controller started");

        Breakpoint.waitUntil("MigratorController:findandlock");
        Breakpoint.resume("MigratorController:findandlock");

        Breakpoint.waitUntil("MigratorController:process");
        System.out.println("Processing");
        FakeMigrator.count = 0;
        Breakpoint.resume("MigratorController:process");

        Breakpoint.waitUntil("MigratorController:unlock");
        // At this point, there must be one TestMigrator instance running
        Assert.assertEquals(1, FakeMigrator.count);
        Breakpoint.resume("MigratorController:unlock");

        controller.getMigrationProcesses().get("customerMigration_0").mig.interrupt();
        controller.interrupt();
        Thread.sleep(100);
    }
}
