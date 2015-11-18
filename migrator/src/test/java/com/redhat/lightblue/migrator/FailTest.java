package com.redhat.lightblue.migrator;

import java.util.Map;

import org.junit.Test;
import org.junit.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.response.LightblueResponse;



import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadJsonNode;


public class FailTest extends AbstractMigratorController {

    private String versionMigrationJob;
    private String versionMigrationConfiguration;
    private String versionSourceCustomer;
    private String versionDestinationCustomer;

    public FailTest() throws Exception {}
        
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
        req.where(Query.withValue("objectType", Query.BinOp.eq, "activeExecution"));
        cli.data(req);
        req=new DataDeleteRequest("migrationJob",null);
        req.where(Query.withValue("objectType", Query.BinOp.eq, "migrationJob"));
        cli.data(req);
        req=new DataDeleteRequest("migrationConfiguration",null);
        req.where(Query.withValue("objectType", Query.BinOp.eq, "migrationConfiguration"));
        cli.data(req);
    }

    @Test
    public void failTest() throws Exception {
        clearData();
        Breakpoint.clearAll();
        loadData("migrationConfiguration", versionMigrationConfiguration, "./test/data/load-migration-configurations-failmigrator.json");
        loadData("migrationJob", versionMigrationJob, "./test/data/load-migration-jobs.json");
        
        MainConfiguration cfg=new MainConfiguration();
        cfg.setName("continuum");
        cfg.setHostName("hostname");
        Controller controller=new Controller(cfg);

        Breakpoint.stop("MigratorController::unlock");

        System.out.println("failTest: Start controller");
        controller.start();
        System.out.println("failTest: controller started");

        Map<String,Controller.MigrationProcess> prc=controller.getMigrationProcesses();

        System.out.println("failTest: wait until unlock");
        Breakpoint.waitUntil("MigratorController:unlock");
        System.out.println("failTest: done");
        // Here, it should have failed already
        LightblueClient cli=new LightblueHttpClient();
        DataFindRequest req=new DataFindRequest("migrationJob",null);
        req.where(Query.withValue("objectType = migrationJob"));
        req.select(Projection.includeFieldRecursively("jobExecutions.0.errorMsg"));
        LightblueResponse resp=cli.data(req);
        JsonNode node=resp.getJson();
        System.out.println("Response:"+node);
        JsonNode x=node.get("processed");
        Assert.assertEquals(1,x.size());
        x=x.get(0).get("jobExecutions").get(0).get("errorMsg");
        Assert.assertNotNull(x.asText());
        System.out.println("Error:"+x.asText());
        Breakpoint.resume("MigratorController:unlock");
        controller.getMigrationProcesses().get("customerMigration_0").mig.interrupt();
        controller.interrupt();
        Thread.sleep(100);
    }
         
}

