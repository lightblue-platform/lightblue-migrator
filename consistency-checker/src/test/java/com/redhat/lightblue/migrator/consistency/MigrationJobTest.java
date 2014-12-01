package com.redhat.lightblue.migrator.consistency;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.response.LightblueResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.redhat.lightblue.util.test.FileUtil.readFile;

public class MigrationJobTest {
    private static final int recordsOverwritten = 42;
    private static final int inconsistentDocuments = 42;
    private static final int consistentDocuments = 21;
    private static final int documentsProcessed = 63;

    MigrationJob migrationJob;

    @Before
    public void setup() {
        migrationJob = new MigrationJob();
        migrationJob = new MigrationJob(new MigrationConfiguration());
        migrationJob.setDocumentsProcessed(documentsProcessed);
        migrationJob.setInconsistentDocuments(inconsistentDocuments);
        migrationJob.setConsistentDocuments(consistentDocuments);
        migrationJob.setRecordsOverwritten(recordsOverwritten);
    }

    @Test
    public void testExecuteExistsInLegacyAndLightblue() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertFalse(migrationJob.hasFailures());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    private List<JsonNode> getProcessedContentsFrom(String filename) {
        List<JsonNode> output = new ArrayList<>();

        JsonNode processedNode = fromFileToJsonNode(filename).findValue("processed");
        if (processedNode instanceof ArrayNode) {
            Iterator<JsonNode> i = ((ArrayNode)processedNode).iterator();
            while (i.hasNext()) {
                output.add(i.next());
            }
        } else {
            output.add(processedNode);
        }

        return output;
    }

    @Test
    public void testExecuteExistsInLegacyButNotLightblue() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertTrue(migrationJob.hasFailures());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteExistsInLegacyButNotLightblueDoNotOverwrite() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        migrationJob.setOverwriteLightblueDocuments(false);
        migrationJob.run();
        Assert.assertTrue(migrationJob.hasFailures());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteExistsInLightblueButNotLegacy() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertFalse(migrationJob.hasFailures());
        Assert.assertEquals(0, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInLegacyAndLightblue() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertFalse(migrationJob.hasFailures());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(2, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInLegacyButNotLightblue() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertTrue(migrationJob.hasFailures());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(2, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(2, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInLightblueButNotLegacy() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("emptyFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertFalse(migrationJob.hasFailures());
        Assert.assertEquals(0, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(0, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteSingleMultipleExistsInLightblueButNotLegacy() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertFalse(migrationJob.hasFailures());
        Assert.assertEquals(1, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(0, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(0, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testExecuteMultipleExistsInLegacyAndSingleExistsInLightblue() {
        MigrationJob migrationJob = new MigrationJob() {
            @Override
            protected List<JsonNode> findLegacyData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("multipleFindResponse.json");
            }

            @Override
            protected List<JsonNode> findLightblueData(LightblueRequest dataRequest) {
                return getProcessedContentsFrom("singleFindResponse.json");
            }

            @Override
            protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
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
        Assert.assertTrue(migrationJob.hasFailures());
        Assert.assertEquals(2, migrationJob.getDocumentsProcessed());
        Assert.assertEquals(1, migrationJob.getConsistentDocuments());
        Assert.assertEquals(1, migrationJob.getInconsistentDocuments());
        Assert.assertEquals(1, migrationJob.getRecordsOverwritten());
    }

    private void configureMigrationJob(MigrationJob migrationJob) {
        MigrationConfiguration jobConfiguration = new MigrationConfiguration();
        jobConfiguration.setLegacyEntityKeyFields(new ArrayList<String>());
        jobConfiguration.setLegacyEntityTimestampField("legacy-timestamp");
        jobConfiguration.setLightblueEntityTimestampField("lightblue-timestamp");
        migrationJob.setJobConfiguration(jobConfiguration);

        migrationJob.setOverwriteLightblueDocuments(true);

        LightblueClient client = new LightblueHttpClient();
        migrationJob.setLegacyClient(client);
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

    @Test
    public void testGetDocumentsProcessed() {
        Assert.assertEquals(documentsProcessed, migrationJob.getDocumentsProcessed());
    }

    @Test
    public void testSetDocumentsProcessed() {
        migrationJob.setDocumentsProcessed(recordsOverwritten);
        Assert.assertEquals(recordsOverwritten, migrationJob.getDocumentsProcessed());
    }

    @Test
    public void testGetInconsistentDocuments() {
        Assert.assertEquals(inconsistentDocuments, migrationJob.getInconsistentDocuments());
    }

    @Test
    public void testSetInconsistentDocuments() {
        migrationJob.setInconsistentDocuments(recordsOverwritten);
        Assert.assertEquals(recordsOverwritten, migrationJob.getInconsistentDocuments());
    }

    @Test
    public void testGetRecordsOverwritten() {
        Assert.assertEquals(recordsOverwritten, migrationJob.getRecordsOverwritten());
    }

    @Test
    public void testSetRecordsOverwritten() {
        migrationJob.setRecordsOverwritten(documentsProcessed);
        Assert.assertEquals(documentsProcessed, migrationJob.getRecordsOverwritten());
    }

}
