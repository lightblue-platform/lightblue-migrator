package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.FileUtil.readFile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class MigrationJobTest {

	private String sourceConfigPath = "source-lightblue-client.properties";
	private String destinationConfigPath = "destination-lightblue-client.properties";
	
	MigrationJob migrationJob;

	@Before
	public void setup() {
		migrationJob = new MigrationJob();
		migrationJob = new MigrationJob(new MigrationConfiguration());
		migrationJob.setSourceConfigPath(sourceConfigPath);
		migrationJob.setDestinationConfigPath(destinationConfigPath);
		migrationJob.setJobRuns(new ArrayList<MigrationJobRun>());
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
			protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
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
		jobConfiguration.setDestinationEntityKeyFields(new ArrayList<String>());
		jobConfiguration.setSourceTimestampPath("source-timestamp");
		migrationJob.setJobConfiguration(jobConfiguration);
		migrationJob.setOverwriteDestinationDocuments(true);
		migrationJob.setJobRuns(new ArrayList<MigrationJobRun>());
		
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
