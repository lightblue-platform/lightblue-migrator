package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.FileUtil.readFile;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class MigrationJobTest {

    private final String sourceConfigPath = "source-lightblue-client.properties";
    private final String destinationConfigPath = "destination-lightblue-client.properties";

    MigrationJob migrationJob;

    @Before
    public void setup() {
        migrationJob = new MigrationJob();
        migrationJob = new MigrationJob(new MigrationConfiguration());
        migrationJob.setSourceConfigPath(sourceConfigPath);
        migrationJob.setDestinationConfigPath(destinationConfigPath);
        migrationJob.setJobExecutions(new ArrayList<MigrationJobExecution>());
    }

    @Test
    public void testSourceGetConfigPath() {
        Assert.assertEquals(sourceConfigPath, migrationJob.getSourceConfigPath());
    }

    @Test
    public void testSetSourceConfigPath() {
        migrationJob.setSourceConfigPath(destinationConfigPath);
        Assert.assertEquals(destinationConfigPath, migrationJob.getSourceConfigPath());
    }

    @Test
    public void testDestinationGetConfigPath() {
        Assert.assertEquals(destinationConfigPath, migrationJob.getDestinationConfigPath());
    }

    @Test
    public void testSetDesitnationConfigPath() {
        migrationJob.setDestinationConfigPath(sourceConfigPath);
        Assert.assertEquals(sourceConfigPath, migrationJob.getDestinationConfigPath());
    }

    /**
     * If source = NullNode and destinationValue = null, then consider equivalent.
     */
    @Test
    public void testDocumentsConsistent_With_Source_NullNode_And_Destination_NullValue(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode sourceNode = factory.objectNode();
        sourceNode.put("somekey", NullNode.getInstance());

        ObjectNode destNode = factory.objectNode();

        assertTrue(migrationJob.documentsConsistent(sourceNode, destNode));
    }

    /**
     * If source = null and destinationValue = NullNode, then consider equivalent.
     */
    @Test
    public void testDocumentsConsistent_With_Source_NullValue_And_Destination_NullNode(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode sourceNode = factory.objectNode();
        String nullString = null;
        sourceNode.put("somekey", nullString);

        ObjectNode destNode = factory.objectNode();
        destNode.put("somekey", NullNode.getInstance());

        assertTrue(migrationJob.documentsConsistent(sourceNode, destNode));
    }

    /**
     * If source = NullNode and destinationValue has a value, then consider inconsistent.
     */
    @Test
    public void testDocumentsConsistent_With_Source_NullNode_But_Destination_HasAValue(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode sourceNode = factory.objectNode();
        sourceNode.put("somekey", NullNode.getInstance());

        ObjectNode destNode = factory.objectNode();
        destNode.put("somekey", factory.textNode("someValue"));

        assertFalse(migrationJob.documentsConsistent(sourceNode, destNode));
    }

    /**
     * If source has a value and destinationValue = NullNode, then consider inconsistent.
     */
    @Test
    public void testDocumentsConsistent_With_Source_HasAValue_But_Destination_NullNode(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode sourceNode = factory.objectNode();
        sourceNode.put("somekey", factory.textNode("someValue"));

        ObjectNode destNode = factory.objectNode();
        destNode.put("somekey", NullNode.getInstance());

        assertFalse(migrationJob.documentsConsistent(sourceNode, destNode));
    }

    /**
     * If source = some value and destinationValue = some other value, then consider equivalent.
     */
    @Test
    public void testDocumentsConsistent_SourceAndDestinationNotConsistent(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode sourceNode = factory.objectNode();
        sourceNode.put("somekey", factory.textNode("faketext"));

        ObjectNode destNode = factory.objectNode();
        destNode.put("somekey", factory.textNode("inconsistentValue"));

        assertFalse(migrationJob.documentsConsistent(sourceNode, destNode));
    }

    @Test
    public void testExecuteExistsInSourceAndDestination() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"OK\"}");
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }

        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    private LinkedHashMap<String, JsonNode> getProcessedContentsFrom(String filename) {
        LinkedHashMap<String, JsonNode> output = new LinkedHashMap<>();

        JsonNode processedNode = fromFileToJsonNode(filename).findValue("processed");
        if (processedNode instanceof ArrayNode) {
            Iterator<JsonNode> i = ((ArrayNode) processedNode).iterator();
            while (i.hasNext()) {
                JsonNode node = i.next();
                output.put(node.findValue("iso3code").textValue(), node);
            }
        } else {
            output.put(processedNode.findValue("iso3code").textValue(), processedNode);
        }

        return output;
    }

    @Test
    public void testExecuteExistsInSourceButNotDestination() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":1,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }
        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteExistsInSourceButNotDestinationDoNotOverwrite() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":1,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }

        };
        configureMigrationJob(migrationJob);
        migrationJob.setOverwriteDestinationDocuments(false);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteExistsInDestinationButNotSource() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }
        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInSourceAndDestination() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }

        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(2, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInSourceButNotDestination() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":2,\"modifiedCount\":2,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }
        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(2, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInDestinationButNotSource() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }
        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleSameExceptForTimestamp() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponseSource.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponseDestination.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }
        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(2, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }


    @Test
    public void testExecuteSingleMultipleExistsInDestinationButNotSource() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }
        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInSourceAndSingleExistsInDestination() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected LinkedHashMap<String, JsonNode> findSourceData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LinkedHashMap<String, JsonNode> findDestinationData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LightblueResponse callLightblue(LightblueRequest saveRequest) {
                LightblueResponse response = new LightblueResponse();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree("{\"errors\":[],\"matchCount\":1,\"modifiedCount\":1,\"status\":\"OK\"}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                response.setJson(node);
                return response;
            }
        };
        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getRecordsOverwritten());
    }

    private void configureMigrationJob(MigrationJob migrationJob) {
        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        List<String> pathsToExclude = new ArrayList<String>();
        pathsToExclude.add("lastUpdateTime");
        jobConfiguration.setComparisonExclusionPaths(pathsToExclude);
        jobConfiguration.setDestinationIdentityFields(new ArrayList<String>());
        jobConfiguration.setSourceTimestampPath("source-timestamp");
        migrationJob.setJobConfiguration(jobConfiguration);
        migrationJob.setOverwriteDestinationDocuments(true);
        migrationJob.setJobExecutions(new ArrayList<MigrationJobExecution>());

        LightblueClient client = new LightblueHttpClient();
        migrationJob.setSourceClient(client);
    }

    private static JsonNode fromFileToJsonNode(String fileName) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = null;
        try {
            actualObj = mapper.readTree(readFile(fileName));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        return actualObj;
    }

}
