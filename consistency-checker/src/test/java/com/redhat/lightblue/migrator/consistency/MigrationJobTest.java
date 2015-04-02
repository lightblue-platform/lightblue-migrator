package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.FileUtil.readFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.response.LightblueResponseParseException;
import com.redhat.lightblue.util.test.FileUtil;

public class MigrationJobTest {

    private final String sourceConfigPath = "./lightblue-client.properties";
    private final String destinationConfigPath = "./lightblue-client.properties";

    protected MigrationJob migrationJob;
    protected LightblueClient sourceClientMock;
    protected LightblueClient destinationClientMock;

    protected class TestMigrationJob extends MigrationJob {
        private final String[] jsonResponses;
        private final String sourceDataResource;
        private final String destinationDataResource;
        private final AtomicInteger callCounter = new AtomicInteger(0);
        private final List<String> requestBodyList = new ArrayList<>();

        private int jsonResponsesPsn = 0;

        /**
         * @param sourceDataResource resource to load for the findSourceData
         * method
         * @param destinationDataResource resource to load for the
         * findDestinationData method
         * @param jsonResponses array of response json (String) to iterate
         * through for callLightblue method, returns last element in array if
         * all are consumed
         */
        public TestMigrationJob(String sourceDataResource, String destinationDataResource, String[] jsonResponses) {
            this.sourceDataResource = sourceDataResource;
            this.destinationDataResource = destinationDataResource;
            this.jsonResponses = jsonResponses;
        }

        @Override
        protected LinkedHashMap<String, JsonNode> findSourceData(AbstractLightblueDataRequest dataRequest) {
            return getProcessedContentsFrom(sourceDataResource);
        }

        @Override
        protected LinkedHashMap<String, JsonNode> findDestinationData(AbstractLightblueDataRequest dataRequest) {
            return getProcessedContentsFrom(destinationDataResource);
        }

        @Override
        protected LightblueResponse callLightblue(AbstractLightblueDataRequest saveRequest) {
            callCounter.incrementAndGet();
            requestBodyList.add(saveRequest.getBody());
            LightblueResponse response = new LightblueResponse();
            JsonNode node = null;
            if (jsonResponses != null && jsonResponsesPsn >= jsonResponses.length) {
                jsonResponsesPsn = jsonResponses.length - 1;
            }
            try {
                node = mapper.readTree(jsonResponses[jsonResponsesPsn++]);
            } catch (IOException e) {
                Assert.fail("Unable to parse json response: " + e.toString());
            }
            response.setJson(node);
            return response;
        }
    };

    @Before
    public void setup() {
        migrationJob = new MigrationJob(new MigrationConfiguration());
        migrationJob.setSourceConfigPath(sourceConfigPath);
        migrationJob.setDestinationConfigPath(destinationConfigPath);
        migrationJob.setJobExecutions(new ArrayList<MigrationJobExecution>());

        // NOTE have to mock source and client, else job initializes clients itself

        // mock out source client
        sourceClientMock = mock(LightblueClient.class);
        migrationJob.setSourceClient(sourceClientMock);

        // mock out destination client
        destinationClientMock = mock(LightblueClient.class);
        migrationJob.setDestinationClient(destinationClientMock);
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
    public void testDocumentsConsistent_With_Source_Null_And_Destination_Null() {
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(null, null);
        assertTrue(inconsistentPaths.isEmpty());
    }

    /**
     * If source has a value and destinationValue = null, then they do not
     * match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Value_And_Destination_NullValue() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(factory.textNode("hi"), null);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
    }

    /**
     * If source = null and destinationValue having a value, then they do not
     * match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_NullValue_And_Destination_Value() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(null, factory.textNode("hi"));
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
    }

    /**
     * If source is an object but destinationValue is not, then they do not
     * match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Object_And_Destination_Not_Object() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(factory.objectNode(), factory.textNode("hi"));
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
    }

    /**
     * Source has more fields than Destination, fail.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Having_More_Fields() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode source = factory.objectNode();
        source.put("fieldName", "fieldValue");
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, factory.objectNode());
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
        assertEquals("fieldName", inconsistentPaths.get(0));
    }

    /**
     * Destination has more fields than Source, fail.
     */
    @Test
    @Ignore
    public void testDocumentsConsistent_With_Destination_Having_More_Fields() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode destination = factory.objectNode();
        destination.put("fieldName", "fieldValue");
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(factory.objectNode(), destination);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
        assertEquals("fieldName", inconsistentPaths.get(0));
    }

    /**
     * Matching object values, pass
     */
    @Test
    public void testDocumentsConsistent_With_Matching_Objects() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode source = factory.objectNode();
        source.put("fieldName", "fieldValue");

        ObjectNode destination = factory.objectNode();
        destination.put("fieldName", "fieldValue");

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, destination);
        assertTrue(inconsistentPaths.isEmpty());
    }

    /**
     * If source is an array but destinationValue is not, then they do not
     * match.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Array_And_Destination_Not_Array() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(factory.arrayNode(), factory.textNode("hi"));
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
    }

    /**
     * Source has more elements than Destination, fail.
     */
    @Test
    public void testDocumentsConsistent_With_Source_Having_More_Elements() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode source = factory.arrayNode();
        source.add(factory.textNode("hi"));
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, factory.arrayNode());
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
    }

    /**
     * Matching array values, pass
     */
    @Test
    public void testDocumentsConsistent_With_Matching_Arrays() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode source = factory.arrayNode();
        source.add(factory.textNode("hi"));

        ArrayNode destination = factory.arrayNode();
        destination.add(factory.textNode("hi"));

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, destination);
        assertTrue(inconsistentPaths.isEmpty());
    }

    /**
     * Array values out of order, fail
     */
    @Test
    public void testDocumentsConsistent_With_Matching_Arrays_But_OutOfSequence() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode source = factory.arrayNode();
        source.add(factory.textNode("hi"));
        source.add(factory.textNode("there"));

        ArrayNode destination = factory.arrayNode();
        destination.add(factory.textNode("there"));
        destination.add(factory.textNode("hi"));

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, destination);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(2, inconsistentPaths.size());
    }

    /**
     * Destination has more elements than Source, fail.
     */
    @Test
    public void testDocumentsConsistent_With_Destination_Having_More_Elements() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ArrayNode destination = factory.arrayNode();
        destination.add(factory.textNode("hi"));
        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(factory.arrayNode(), destination);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
    }

    /**
     * If source = some value and destinationValue = some other value, then
     * consider equivalent.
     */
    @Test
    public void testDocumentsConsistent_SourceAndDestinationNotConsistent() {
        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        ObjectNode sourceNode = factory.objectNode();
        sourceNode.put("somekey", factory.textNode("faketext"));

        ObjectNode destNode = factory.objectNode();
        destNode.put("somekey", factory.textNode("inconsistentValue"));

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(sourceNode, destNode);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
        assertEquals("somekey", inconsistentPaths.get(0));
    }

    /**
     * Technically the two values do not match, however the field in question is
     * excluded and should pass.
     */
    @Test
    public void testDocumentsConsistent_WithMultiLevelExclude() {
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

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(sourceNode, destNode);
        assertTrue(inconsistentPaths.isEmpty());
    }

    /**
     * Ensures that a more complex json document, with a deep invalid field but
     * where that field is excluded. Should pass.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_Pass() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}");

        JsonNode dest = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("array.something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, dest);
        assertTrue(inconsistentPaths.isEmpty());
    }

    /**
     * Ensures that a more complex json document, with a deep invalid field.
     * Should fail.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_Fail() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}");

        JsonNode dest = mapper.readTree(
                "{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}");

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, dest);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
        assertEquals("array.something", inconsistentPaths.get(0));
    }

    /**
     * Ensures that a more complex json document with a top level array, with a
     * deep invalid field but where that field is excluded. Should pass.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelArray_Pass() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}]");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("array.something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, dest);
        assertTrue(inconsistentPaths.isEmpty());
    }

    /**
     * Ensures that a more complex json document with a top level array, with a
     * deep invalid field. Should fail.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelArray_Fail() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}]");

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, dest);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
        assertEquals("array.something", inconsistentPaths.get(0));
    }

    /**
     * Both something fields are mismatched, but only the top level is excluded.
     * Should fail.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelArray_ExcludeNotOnField_Fail() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"y\", \"array\": [{\"field2\": \"value2\",\"something\": \"z\"}]}]");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, dest);
        assertFalse(inconsistentPaths.isEmpty());
        assertEquals(1, inconsistentPaths.size());
        assertEquals("array.something", inconsistentPaths.get(0));
    }

    /**
     * Top level something field is mismatched, but is also excluded. Should
     * pass.
     */
    @Test
    public void testDocumentsConsistent_ComplexJson_WithTopLevelMismatch_fail() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode source = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"x\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        JsonNode dest = mapper.readTree(
                "[{\"field1\": \"value1\", \"something\": \"y\", \"array\": [{\"field2\": \"value2\",\"something\": \"y\"}]}]");

        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setComparisonExclusionPaths(Arrays.asList("something"));
        migrationJob.setJobConfiguration(jobConfiguration);

        List<String> inconsistentPaths = migrationJob.getInconsistentPaths(source, dest);
        assertTrue(inconsistentPaths.isEmpty());
    }

    @Test
    public void testGetDestinationDocuments_UnderBatchingLimit() throws IOException {
        String key = "id";
        String value = "uniqueId";

        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        migrationJob.getJobConfiguration().setDestinationIdentityFields(Arrays.asList(key));

        ObjectNode document = factory.objectNode();
        document.put(key, factory.textNode(value));

        Map<String, JsonNode> sourceDocuments = new HashMap<>();
        sourceDocuments.put(value, document);

        JsonNode[] destinationDocuments = new JsonNode[]{document};
        when(destinationClientMock.data(any(AbstractLightblueDataRequest.class), eq(JsonNode[].class))).thenReturn(destinationDocuments);

        Map<String, JsonNode> actual = migrationJob.getDestinationDocuments(sourceDocuments);

        assertNotNull(actual);
        assertTrue(actual.containsKey("\"" + value + "\"|||"));

        assertEquals(sourceDocuments.get(value), actual.get("\"" + value + "\"|||"));
    }

    @Test
    public void testGetDestinationDocuments_OverBatchingLimit_MultipleOfLimit() throws IOException {
        String key = "id";
        String value = "uniqueId";

        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        migrationJob.getJobConfiguration().setDestinationIdentityFields(Arrays.asList(key));

        Map<String, JsonNode> sourceDocuments = new HashMap<>();
        for (int x = 0; x < (MigrationJob.BATCH_SIZE * 2); x++) {
            ObjectNode document = factory.objectNode();
            document.put(key, factory.textNode(value + x));
            sourceDocuments.put(value + x, document);
        }

        ObjectNode dd1 = factory.objectNode();
        dd1.put(key, value + 1);
        JsonNode[] destinationDocumentsBatch1 = new JsonNode[]{dd1};
        ObjectNode dd2 = factory.objectNode();
        dd2.put(key, value + 101);
        JsonNode[] destinationDocumentsBatch2 = new JsonNode[]{dd2};
        when(destinationClientMock.data(any(AbstractLightblueDataRequest.class), eq(JsonNode[].class)))
        .thenReturn(destinationDocumentsBatch1)
        .thenReturn(destinationDocumentsBatch2);

        Map<String, JsonNode> actual = migrationJob.getDestinationDocuments(sourceDocuments);

        assertNotNull(actual);
        assertTrue(actual.containsKey("\"" + value + "1\"|||"));
        assertTrue(actual.containsKey("\"" + value + "101\"|||"));
    }

    @Test
    public void testGetDestinationDocuments_OverBatchingLimit() throws IOException {
        String key = "id";
        String value = "uniqueId";

        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);
        migrationJob.getJobConfiguration().setDestinationIdentityFields(Arrays.asList(key));

        Map<String, JsonNode> sourceDocuments = new HashMap<>();
        for (int x = 0; x < (MigrationJob.BATCH_SIZE + 1); x++) {
            ObjectNode document = factory.objectNode();
            document.put(key, factory.textNode(value + x));
            sourceDocuments.put(value + x, document);
        }

        ObjectNode dd1 = factory.objectNode();
        dd1.put(key, value + 1);
        JsonNode[] destinationDocumentsBatch1 = new JsonNode[]{dd1};
        ObjectNode dd2 = factory.objectNode();
        dd2.put(key, value + 101);
        JsonNode[] destinationDocumentsBatch2 = new JsonNode[]{dd2};
        when(destinationClientMock.data(any(AbstractLightblueDataRequest.class), eq(JsonNode[].class)))
        .thenReturn(destinationDocumentsBatch1)
        .thenReturn(destinationDocumentsBatch2);

        Map<String, JsonNode> actual = migrationJob.getDestinationDocuments(sourceDocuments);

        assertNotNull(actual);
        assertTrue(actual.containsKey("\"" + value + "1\"|||"));
        assertTrue(actual.containsKey("\"" + value + "101\"|||"));
    }

    @Test
    public void testGetDestinationDocuments_NullMap() throws IOException {
        assertTrue(migrationJob.getDestinationDocuments(null).isEmpty());
    }

    @Test
    public void testGetDestinationDocuments_EmptyMap() throws IOException {
        assertTrue(migrationJob.getDestinationDocuments(new HashMap<String, JsonNode>()).isEmpty());
    }

    @Test
    public void testOverwriteLightblue_OverBatchingLimit_MultipleOfLimit() throws IOException {
        String key = "id";
        String value = "uniqueId";

        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);

        List<JsonNode> documentsToOverwrite = new ArrayList<>();
        int documentCount = MigrationJob.BATCH_SIZE * 2;
        for (int x = 0; x < documentCount; x++) {
            ObjectNode document = factory.objectNode();
            document.put(key, factory.textNode(value + x));
            documentsToOverwrite.add(document);
        }

        when(destinationClientMock.data(any(AbstractLightblueDataRequest.class))).thenReturn(new LightblueResponse("{\"modifiedCount\":2}"));

        List<LightblueResponse> responses = migrationJob.overwriteLightblue(documentsToOverwrite);

        Assert.assertNotNull(responses);
        Assert.assertEquals((int) Math.ceil(((double) documentCount) / (double) MigrationJob.BATCH_SIZE), responses.size());
    }

    @Test
    public void testOverwriteLightblue_OverBatchingLimit() throws IOException {
        String key = "id";
        String value = "uniqueId";

        JsonNodeFactory factory = JsonNodeFactory.withExactBigDecimals(false);

        List<JsonNode> documentsToOverwrite = new ArrayList<>();
        int documentCount = MigrationJob.BATCH_SIZE + 1;
        for (int x = 0; x < documentCount; x++) {
            ObjectNode document = factory.objectNode();
            document.put(key, factory.textNode(value + x));
            documentsToOverwrite.add(document);
        }

        when(destinationClientMock.data(any(AbstractLightblueDataRequest.class))).thenReturn(new LightblueResponse("{\"modifiedCount\":2}"));

        List<LightblueResponse> responses = migrationJob.overwriteLightblue(documentsToOverwrite);

        Assert.assertNotNull(responses);
        Assert.assertEquals((int) Math.ceil(((double) documentCount) / (double) MigrationJob.BATCH_SIZE), responses.size());
    }

    @Test
    public void testExecuteExistsInSourceAndDestination() {
        TestMigrationJob migrationJob = new TestMigrationJob("singleFindResponse.json", "singleFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"COMPLETE\",\"processed\":[{}]}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(3, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(2).contains(JobStatus.COMPLETED_SUCCESS.toString()));
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
        TestMigrationJob migrationJob = new TestMigrationJob("singleFindResponse.json", "emptyFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":1,\"status\":\"COMPLETE\",\"processed\":[{}]}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(4, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        // index 2 is saving destination data
        Assert.assertTrue(migrationJob.requestBodyList.get(3).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteExistsInSourceButNotDestinationDoNotOverwrite() {
        TestMigrationJob migrationJob = new TestMigrationJob("singleFindResponse.json", "emptyFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":1,\"status\":\"COMPLETE\",\"processed\":[{}]}"});

        configureMigrationJob(migrationJob);
        migrationJob.setOverwriteDestinationDocuments(false);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(3, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(2).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteExistsInDestinationButNotSource() {
        TestMigrationJob migrationJob = new TestMigrationJob("emptyFindResponse.json", "singleFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"COMPLETE\",\"processed\":[{}]}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(3, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(2).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteMultipleExistsInSourceAndDestination() {
        TestMigrationJob migrationJob = new TestMigrationJob("multipleFindResponse.json", "multipleFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"COMPLETE\",\"processed\":[{}]}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(2, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());

        // verify source query is set
        Assert.assertNotNull(migrationJob.getJobExecutions());
        Assert.assertEquals(1, migrationJob.getJobExecutions().size());
        Assert.assertNotNull(migrationJob.getJobExecutions().get(0).getSourceQuery());

        // verify statuses
        Assert.assertEquals(3, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(2).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteMultipleExistsInSourceButNotDestination() {
        TestMigrationJob migrationJob = new TestMigrationJob("multipleFindResponse.json", "emptyFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":2,\"modifiedCount\":2,\"status\":\"COMPLETE\",\"processed\":[{}]}}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(2, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(4, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        // index 2 is data save request
        Assert.assertTrue(migrationJob.requestBodyList.get(3).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteMultipleExistsInDestinationButNotSource() {
        TestMigrationJob migrationJob = new TestMigrationJob("emptyFindResponse.json", "multipleFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"COMPLETE\",\"processed\":[{}]}}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(3, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(2).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteMultipleSameExceptForTimestamp() {
        TestMigrationJob migrationJob = new TestMigrationJob("multipleFindResponseSource.json", "multipleFindResponseDestination.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"COMPLETE\",\"processed\":[{}]}}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(2, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(3, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(2).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteSingleMultipleExistsInDestinationButNotSource() {
        TestMigrationJob migrationJob = new TestMigrationJob("singleFindResponse.json", "multipleFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":0,\"modifiedCount\":0,\"status\":\"COMPLETE\",\"processed\":[{}]}}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(3, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(2).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    @Test
    public void testExecuteMultipleExistsInSourceAndSingleExistsInDestination() {
        TestMigrationJob migrationJob = new TestMigrationJob("multipleFindResponse.json", "singleFindResponse.json",
                new String[]{"{\"errors\":[],\"matchCount\":1,\"modifiedCount\":1,\"status\":\"COMPLETE\",\"processed\":[{}]}}"});

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getRecordsOverwritten());
        Assert.assertEquals(4, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        // index 2 is data save request
        Assert.assertTrue(migrationJob.requestBodyList.get(3).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    private void configureMigrationJob(MigrationJob migrationJob) {
        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        List<String> pathsToExclude = new ArrayList<>();
        pathsToExclude.add("lastUpdateTime");
        jobConfiguration.setComparisonExclusionPaths(pathsToExclude);
        jobConfiguration.setDestinationIdentityFields(new ArrayList<String>());
        jobConfiguration.setSourceTimestampPath("sourceTimestamp");
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
            Assert.fail("Unable to parse json response: " + e.toString());
        }
        return actualObj;
    }

    /**
     * This job is the first of two to execute and therefore should process the
     * job.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleJobExecutors_first() throws Exception {
        TestMigrationJob migrationJob = new TestMigrationJob("multipleFindResponseSource.json", "singleFindResponse.json",
                new String[]{
                        // initial job save
                        FileUtil.readFile("migrationJobTwoExecutionsResponse.json"),
                        // update job to running
                        FileUtil.readFile("migrationJobTwoExecutionsResponse.json"),
                        // save one document
                        "{\"errors\":[],\"matchCount\":1,\"modifiedCount\":1,\"status\":\"COMPLETE\",\"processed\":[{}]}",
                        // final job save
                        FileUtil.readFile("migrationJobTwoExecutionsResponse.json")
        });

        // this job will be the first executor and therefore wins
        migrationJob.setPid("pid1");

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getRecordsOverwritten());
        // verify marked complete
        Assert.assertNotNull(migrationJob.currentRun.getActualEndDate());
        Assert.assertEquals(4, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.RUNNING.toString()));
        // index 2 is data save request
        Assert.assertTrue(migrationJob.requestBodyList.get(3).contains(JobStatus.COMPLETED_SUCCESS.toString()));
    }

    /**
     * This job is the second to attempt to process it and therefore it should
     * not actually do anything.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleJobExecutors_second() throws Exception {
        TestMigrationJob migrationJob = new TestMigrationJob("multipleFindResponseSource.json", "singleFindResponse.json",
                new String[]{
                        // initial job save
                        FileUtil.readFile("migrationJobTwoExecutionsResponse.json"),
                        // final job save (since this is a noop, there is nothing in the middle)
                        FileUtil.readFile("migrationJobTwoExecutionsResponse.json")
        });

        // this job will be the second executor and therefore looses
        migrationJob.setPid("pid2");

        configureMigrationJob(migrationJob);
        migrationJob.run();
        Assert.assertFalse(migrationJob.hasInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());

        Assert.assertNotNull(migrationJob.currentRun.getActualEndDate());

        Assert.assertEquals(2, migrationJob.callCounter.get());
        Assert.assertTrue(migrationJob.requestBodyList.get(0).contains(JobStatus.STARTING.toString()));
        Assert.assertTrue(migrationJob.requestBodyList.get(1).contains(JobStatus.ABORTED_DUPLICATE.toString()));
    }

    /**
     * Thread starvation:
     * https://github.com/lightblue-platform/lightblue-migrator/issues/129
     *
     * Test with ExecutorService#submit
     */
    @Test
    public void testException_getSourceDocuments_submit() throws IOException, URISyntaxException, InterruptedException {
        final int JOB_COUNT = 100;
        final int THREAD_COUNT = 2;
        final AtomicInteger outstandingThreadCount = new AtomicInteger(0);

        // create a job executor with more jobs than threads
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < JOB_COUNT; i++) {
            outstandingThreadCount.getAndIncrement();
            MigrationJob job = new TestMigrationJob(null, null,
                    new String[]{
                            FileUtil.readFile("migrationJobTwoExecutionsResponse.json")
            }) {
                @Override
                protected Map<String, JsonNode> getSourceDocuments() throws SQLException {
                    outstandingThreadCount.getAndDecrement();
                    throw new SQLException("forced failure for testing");
                }
            };

            job.setPid("pid1");

            futures.add(executor.submit(job));
        }

        Assert.assertTrue(JOB_COUNT > THREAD_COUNT);

        // all jobs are created.  There are JOB_COUNT of them but only THREAD_COUNT threads
        // but we want to be sure to have *some* jobs to run before we wait for termination
        Assert.assertTrue(outstandingThreadCount.intValue() > (JOB_COUNT / 5));
        for (Future future : futures) {
            try {
                future.get();
            } catch (ExecutionException ex) {
                Assert.fail(ex.getCause().toString());
            }
        }
        // and now that we're done, verify everything got processed
        Assert.assertEquals(0, outstandingThreadCount.intValue());
    }

    /**
     * Thread starvation:
     * https://github.com/lightblue-platform/lightblue-migrator/issues/129
     *
     * Test with ExecutorService#execute
     */
    @Test
    public void testException_getSourceDocuments_execute() throws IOException, URISyntaxException, InterruptedException {
        final int JOB_COUNT = 100;
        final int THREAD_COUNT = 2;
        final AtomicInteger outstandingThreadCount = new AtomicInteger(0);

        // create a job executor with more jobs than threads
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < JOB_COUNT; i++) {
            outstandingThreadCount.getAndIncrement();
            MigrationJob job = new TestMigrationJob(null, null,
                    new String[]{
                            FileUtil.readFile("migrationJobTwoExecutionsResponse.json")
            }) {
                @Override
                protected Map<String, JsonNode> getSourceDocuments() throws SQLException {
                    outstandingThreadCount.getAndDecrement();
                    throw new SQLException("forced failure for testing");
                }
            };

            job.setPid("pid1");

            executor.execute(job);
        }

        Assert.assertTrue(JOB_COUNT > THREAD_COUNT);

        executor.shutdown();

        // all jobs are created.  There are JOB_COUNT of them but only THREAD_COUNT threads
        // but we want to be sure to have *some* jobs to run before we wait for termination
        Assert.assertTrue(outstandingThreadCount.intValue() > (JOB_COUNT / 2));

        executor.awaitTermination(60, TimeUnit.SECONDS);

        // and now that we're done, verify everything got processed
        Assert.assertEquals(0, outstandingThreadCount.intValue());
    }

    @Test
    public void shouldProcessJob_NoExecutions() throws LightblueResponseParseException {
        Object[] x = MigrationJob.shouldProcessJob("", new MigrationJob[]{migrationJob});
        boolean processJob = (Boolean) x[0];
        int jobExecutionPsn = (Integer) x[1];

        Assert.assertTrue(processJob);
        Assert.assertEquals(-1, jobExecutionPsn);
    }

    @Test
    public void shouldProcessJob_OneExecution_NotCompleted_pidMatch() throws LightblueResponseParseException {
        String pid = "asdf";

        migrationJob.setPid(pid);
        MigrationJobExecution exec = new MigrationJobExecution();
        exec.setJobStatus(JobStatus.RUNNING);
        exec.setPid(pid);
        List<MigrationJobExecution> execs = new ArrayList<>();
        execs.add(exec);
        migrationJob.setJobExecutions(execs);
        Object[] x = MigrationJob.shouldProcessJob(pid, new MigrationJob[]{migrationJob});
        boolean processJob = (Boolean) x[0];
        int jobExecutionPsn = (Integer) x[1];

        Assert.assertTrue(processJob);
        Assert.assertEquals(0, jobExecutionPsn);
    }

    @Test
    public void shouldProcessJob_OneExecution_NotCompleted_pidMismatch() throws LightblueResponseParseException {
        String pid = "asdf";

        migrationJob.setPid(pid);
        MigrationJobExecution exec = new MigrationJobExecution();
        exec.setJobStatus(JobStatus.RUNNING);
        exec.setPid(pid + "-mismatch");
        List<MigrationJobExecution> execs = new ArrayList<>();
        execs.add(exec);
        migrationJob.setJobExecutions(execs);
        Object[] x = MigrationJob.shouldProcessJob(pid, new MigrationJob[]{migrationJob});
        boolean processJob = (Boolean) x[0];
        int jobExecutionPsn = (Integer) x[1];

        Assert.assertFalse(processJob);
        Assert.assertEquals(-1, jobExecutionPsn);
    }
}
