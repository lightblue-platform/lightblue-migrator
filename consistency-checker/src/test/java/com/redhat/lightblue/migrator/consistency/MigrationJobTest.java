package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.FileUtil.readFile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class MigrationJobTest {
	private static final int recordsOverwritten = 42;
	private static final int inconsistentDocuments = 42;
	private static final int consistentDocuments = 21;
	private static final int documentsProcessed = 63;
	
	MigrationJob migrationJob;
	
	@Before
	public void setup() {
		migrationJob = new MigrationJob();
		migrationJob = new MigrationJob(new JobConfiguration());
		migrationJob.setDocumentsProcessed(documentsProcessed);
		migrationJob.setInconsistentDocuments(inconsistentDocuments);
		migrationJob.setConsistentDocuments(consistentDocuments);
		migrationJob.setRecordsOverwritten(recordsOverwritten);
	}
	
	@Test
	public void testExecuteExistsInLegacyAndLightblue() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

		};
		configureCommand(migrationJob);
		migrationJob.run();
		Assert.assertFalse(migrationJob.hasFailures());
	}

	@Test
	public void testExecuteExistsInLegacyButNotLightblue() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse saveLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(migrationJob);
		migrationJob.run();
		Assert.assertTrue(migrationJob.hasFailures());
	}

	@Test
	public void testExecuteExistsInLegacyButNotLightblueDoNotOverwrite() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}
		};
		configureCommand(migrationJob);
		migrationJob.setOverwriteLightblueDocuments(false);
		migrationJob.run();
		Assert.assertTrue(migrationJob.hasFailures());
	}

	@Test
	public void testExecuteExistsInLightblueButNotLegacy() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse saveLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(migrationJob);
		migrationJob.run();
		Assert.assertTrue(migrationJob.hasFailures());
	}

	@Test
	public void testExecuteMultipleExistsInLegacyAndLightblue() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}
		};
		configureCommand(migrationJob);
		migrationJob.run();
		Assert.assertFalse(migrationJob.hasFailures());
	}

	@Test
	public void testExecuteMultipleExistsInLegacyButNotLightblue() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse saveLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(migrationJob);
		migrationJob.run();
		Assert.assertTrue(migrationJob.hasFailures());
	}

	@Test
	public void testExecuteMultipleExistsInLightblueButNotLegacy() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse saveLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(migrationJob);
		migrationJob.run();
		Assert.assertTrue(migrationJob.hasFailures());
	}

	@Test
	public void testExecuteSingleMultipleExistsInLightblueButNotLegacy() {
		MigrationJob migrationJob = new MigrationJob() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse saveLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(migrationJob);
		migrationJob.run();
		Assert.assertTrue(migrationJob.hasFailures());
	}

	private void configureCommand(MigrationJob migrationJob) {
		JobConfiguration jobConfiguration = new JobConfiguration();
		jobConfiguration.setLegacyEntityKeyFields(new ArrayList<String>());
		migrationJob.setJobConfiguration(jobConfiguration);
		migrationJob.setOverwriteLightblueDocuments(true);
		
		LightblueClient client = new LightblueHttpClient();
		migrationJob.setClient(client);
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
