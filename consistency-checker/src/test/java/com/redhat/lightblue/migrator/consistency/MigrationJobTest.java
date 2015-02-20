package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.FileUtil.readFile;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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
     * If source and destination = null, then they match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Null_And_Destination_Null(){
        assertTrue(migrationJob.documentsConsistent(null, null));
    }

    /**
     * If source has a value and destinationValue = null, then they do not match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Value_And_Destination_NullValue(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        assertFalse(migrationJob.documentsConsistent(factory.textNode("hi"), null));
    }

    /**
     * If source = null and destinationValue having a value, then they do not match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_NullValue_And_Destination_Value(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        assertFalse(migrationJob.documentsConsistent(null, factory.textNode("hi")));
    }

    /**
     * If source is an object but destinationValue is not, then they do not match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Object_And_Destination_Not_Object(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        assertFalse(migrationJob.documentsConsistent(factory.objectNode(), factory.textNode("hi")));
    }

    /**
     * Source has more fields than Destination, fail.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Having_More_Fields(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode source = factory.objectNode();
        source.put("fieldName", "fieldValue");
        assertFalse(migrationJob.documentsConsistent(source, factory.objectNode()));
    }

    /**
     * Destination has more fields than Source, fail.
     */
    @Test
    @Ignore
    public void testDocumentsConsistent_With_Destination_Having_More_Fields(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode destination = factory.objectNode();
        destination.put("fieldName", "fieldValue");
        assertFalse(migrationJob.documentsConsistent(factory.objectNode(), destination));
    }

    /**
     * Matching object values, pass
     */
    @Test
    public void testDocumentsConsistent_With_Matching_Objects(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode source = factory.objectNode();
        source.put("fieldName", "fieldValue");

        ObjectNode destination = factory.objectNode();
        destination.put("fieldName", "fieldValue");

        assertTrue(migrationJob.documentsConsistent(source, destination));
    }

    /**
     * If source is an array but destinationValue is not, then they do not match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Array_And_Destination_Not_Array(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        assertFalse(migrationJob.documentsConsistent(factory.arrayNode(), factory.textNode("hi")));
    }

    /**
     * Source has more elements than Destination, fail.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Having_More_Elements(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode source = factory.arrayNode();
        source.add(factory.textNode("hi"));
        assertFalse(migrationJob.documentsConsistent(source, factory.arrayNode()));
    }

    /**
     * Matching array values, pass
     */
    @Test
    public void testDocumentsConsistent_With_Matching_Arrays(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode source = factory.arrayNode();
        source.add(factory.textNode("hi"));

        ArrayNode destination = factory.arrayNode();
        destination.add(factory.textNode("hi"));

        assertTrue(migrationJob.documentsConsistent(source, destination));
    }

    /**
     * Array values out of order, fail
     */
    @Test
    public void testDocumentsConsistent_With_Matching_Arrays_But_OutOfSequence(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode source = factory.arrayNode();
        source.add(factory.textNode("hi"));
        source.add(factory.textNode("there"));

        ArrayNode destination = factory.arrayNode();
        destination.add(factory.textNode("there"));
        destination.add(factory.textNode("hi"));

        assertFalse(migrationJob.documentsConsistent(source, destination));
    }

    /**
     * Destination has more elements than Source, fail.
     */
    @Test
    public void testDocumentsConsistent_With_Destination_Having_More_Elements(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode destination = factory.arrayNode();
        destination.add(factory.textNode("hi"));
        assertFalse(migrationJob.documentsConsistent(factory.arrayNode(), destination));
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

    /**
     * Technically the two values do not match, however the field in question
     * is excluded and should pass.
     */
    @Test
    public void testDocumentsConsistent_WithMultiLevelExclude(){
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);

        ObjectNode sourceChild = factory.objectNode();
        sourceChild.put("someChildKey", "hi");

        ObjectNode sourceNode = factory.objectNode();
        sourceNode.put("somekey", sourceChild);

        ObjectNode destChild = factory.objectNode();
        destChild.put("someChildKey", "there");

        ObjectNode destNode = factory.objectNode();
        destNode.put("somekey", destChild);

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("somekey.someChildKey"));
        migrationJob.setJobConfiguration(jobConfiguration);

        assertTrue(migrationJob.documentsConsistent(sourceNode, destNode));
    }

    /**
     * Ensures that a more complex json document, with a deep invalid field but where that field is excluded.
     * Should pass.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_Pass() throws Exception{
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}");

        JsonNode dest = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("array.something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        assertTrue(migrationJob.documentsConsistent(source, dest));
    }

    /**
     * Ensures that a more complex json document, with a deep invalid field.
     * Should fail.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_Fail() throws Exception{
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}");

        JsonNode dest = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}");

        assertFalse(migrationJob.documentsConsistent(source, dest));
    }

    /**
     * Ensures that a more complex json document with a top level array, with a deep invalid field but where that field is excluded.
     * Should pass.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelArray_Pass() throws Exception{
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}]");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("array.something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        assertTrue(migrationJob.documentsConsistent(source, dest));
    }

    /**
     * Ensures that a more complex json document with a top level array, with a deep invalid field.
     * Should fail.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelArray_Fail() throws Exception{
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}]");

        assertFalse(migrationJob.documentsConsistent(source, dest));
    }

    /**
     * Both something fields are mismatched, but only the top level is excluded.
     * Should fail.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelArray_ExcludeNotOnField_Fail() throws Exception{
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"y\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}]");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        assertFalse(migrationJob.documentsConsistent(source, dest));
    }

    /**
     * Top level something field is mismatched, but is also excluded.
     * Should pass.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelMismatch_fail() throws Exception{
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"y\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        assertTrue(migrationJob.documentsConsistent(source, dest));
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
