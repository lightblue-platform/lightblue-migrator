package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.util.test.FileUtil.readFile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class CompareLightblueToLegacyCommandExecuteTest {

	@Test
	public void testExecuteExistsInLegacyAndLightblue() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

		};
		configureCommand(command);
		command.execute();
		Assert.assertFalse(command.hasFailures());
	}

	@Test
	public void testExecuteExistsInLegacyButNotLightblue() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse updateLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(command);
		command.execute();
		Assert.assertTrue(command.hasFailures());
	}

	@Test
	public void testExecuteExistsInLegacyButNotLightblueDoNotOverwrite() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}
		};
		configureCommand(command);
		command.setOverwriteLightblueDocuments(false);
		command.execute();
		Assert.assertTrue(command.hasFailures());
	}

	@Test
	public void testExecuteExistsInLightblueButNotLegacy() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse updateLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(command);
		command.execute();
		Assert.assertTrue(command.hasFailures());
	}

	@Test
	public void testExecuteMultipleExistsInLegacyAndLightblue() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}
		};
		configureCommand(command);
		command.execute();
		Assert.assertFalse(command.hasFailures());
	}

	@Test
	public void testExecuteMultipleExistsInLegacyButNotLightblue() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse updateLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(command);
		command.execute();
		Assert.assertTrue(command.hasFailures());
	}

	@Test
	public void testExecuteMultipleExistsInLightblueButNotLegacy() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("emptyFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse updateLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(command);
		command.execute();
		Assert.assertTrue(command.hasFailures());
	}

	@Test
	public void testExecuteSingleMultipleExistsInLightblueButNotLegacy() {
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand() {
			@Override
			protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("singleFindResponse.json").findValues("processed");
			}

			@Override
			protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
				return fromFileToJsonNode("multipleFindResponse.json").findValues("processed");
			}

			@Override
			protected LightblueResponse updateLightblueData(LightblueRequest updateRequest) {
				return new LightblueResponse();
			}
		};
		configureCommand(command);
		command.execute();
		Assert.assertTrue(command.hasFailures());
	}

	private void configureCommand(CompareLightblueToLegacyCommand command) {
		command.setOverwriteLightblueDocuments(true);
		command.setLightblueSaveJsonExpression("{$nodeData}");
		LightblueClient client = new LightblueHttpClient();
		command.setClient(client);
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
