package com.redhat.lightblue.migrator.consistency;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class CompareLightblueToLegacyCommand {
	public static final Log LOG = LogFactory.getLog("CompareLightblueToLegacyCommand");

	LightblueClient client = new LightblueHttpClient();

	private int documentsCompared = 0;
	private int inconsistentDocuments = 0;
	private int lightblueDocumentsUpdated = 0;
	private String lightblueEntityName;
	private String lightblueEntityVersion;
	private String legacyEntityName;
	private String legacyEntityVersion;
	private String lightblueServiceURI;
	private String legacyServiceURI;
	private String legacyFindJsonExpression;
	private String lightblueFindJsonExpression;
	private String lightblueUpdateJsonExpression;
	private boolean hasFailures = false;
	private boolean overwriteLightblueDocuments = false;

	public int getDocumentsCompared() {
		return documentsCompared;
	}

	public void setDocumentsCompared(int documentsCompared) {
		this.documentsCompared = documentsCompared;
	}

	public int getInconsistentDocuments() {
		return inconsistentDocuments;
	}

	public void setInconsistentDocuments(int inconsistentDocuments) {
		this.inconsistentDocuments = inconsistentDocuments;
	}

	public int getLightblueDocumentsUpdated() {
		return lightblueDocumentsUpdated;
	}

	public void setLightblueDocumentsUpdated(int lightblueDocumentsUpdated) {
		this.lightblueDocumentsUpdated = lightblueDocumentsUpdated;
	}

	public String getLightblueEntityName() {
		return lightblueEntityName;
	}

	public void setLightblueEntityName(String lightblueEntityName) {
		this.lightblueEntityName = lightblueEntityName;
	}

	public String getLightblueEntityVersion() {
		return lightblueEntityVersion;
	}

	public void setLightblueEntityVersion(String lightblueEntityVersion) {
		this.lightblueEntityVersion = lightblueEntityVersion;
	}

	public String getLegacyEntityName() {
		return legacyEntityName;
	}

	public void setLegacyEntityName(String legacyEntityName) {
		this.legacyEntityName = legacyEntityName;
	}

	public String getLegacyEntityVersion() {
		return legacyEntityVersion;
	}

	public void setLegacyEntityVersion(String legacyEntityVersion) {
		this.legacyEntityVersion = legacyEntityVersion;
	}

	public String getLegacyFindJsonExpression() {
		return legacyFindJsonExpression;
	}

	public void setLegacyFindJsonExpression(String legacyFindJsonExpression) {
		this.legacyFindJsonExpression = legacyFindJsonExpression;
	}

	public String getLightblueFindJsonExpression() {
		return lightblueFindJsonExpression;
	}

	public void setLightblueFindJsonExpression(String lightblueFindJsonExpression) {
		this.lightblueFindJsonExpression = lightblueFindJsonExpression;
	}

	public String getLightblueUpdateJsonExpression() {
		return lightblueUpdateJsonExpression;
	}

	public void setLightblueUpdateJsonExpression(String lightblueUpdateJsonExpression) {
		this.lightblueUpdateJsonExpression = lightblueUpdateJsonExpression;
	}

	public boolean hasFailures() {
		return hasFailures;
	}

	public void setHasFailures(boolean hasFailures) {
		this.hasFailures = hasFailures;
	}

	public LightblueClient getClient() {
		return client;
	}

	public void setClient(LightblueClient client) {
		this.client = client;
	}

	public String getLightblueServiceURI() {
		return lightblueServiceURI;
	}

	public void setLightblueServiceURI(String lightblueServiceURI) {
		this.lightblueServiceURI = lightblueServiceURI;
	}

	public String getLegacyServiceURI() {
		return legacyServiceURI;
	}

	public void setLegacyServiceURI(String legacyServiceURI) {
		this.legacyServiceURI = legacyServiceURI;
	}

	public void setOverwriteLightblueDocuments(boolean overwriteLightblueDocuments) {
		this.overwriteLightblueDocuments = overwriteLightblueDocuments;
	}

	public boolean shouldOverwriteLightblueDocuments() {
		return overwriteLightblueDocuments;
	}

	public boolean getOverwriteLightblueDocuments() {
		return this.overwriteLightblueDocuments;
	}

	public void execute() {

		LOG.info("CompareLightblueToLegacyCommand started");

		List<JsonNode> legacyDocumentsToCompare = getLegacyDocuments();
		compareLegacyToLightblue(legacyDocumentsToCompare);

		LOG.info("CompareLightblueToLegacyCommand completed");
	}

	private List<JsonNode> getLegacyDocuments() {
		List<JsonNode> documents = new ArrayList<JsonNode>();
		LightblueRequest legacyRequest = new DataFindRequest(legacyEntityName, legacyEntityVersion);
		legacyRequest.setBody(legacyFindJsonExpression);
		documents = getClient().data(legacyRequest).getJson().findValues("processed");
		return documents;
	}

	private void compareLegacyToLightblue(List<JsonNode> legacyDocuments) {

		for (JsonNode node : legacyDocuments) {
			LightblueRequest lightblueRequest = new DataFindRequest(lightblueEntityName, lightblueEntityVersion);
			lightblueRequest.setBody(legacyFindJsonExpression);
			LightblueResponse response = getClient().data(lightblueRequest);
			if (!node.equals(response.getJson())) {
				hasFailures = true;
				LOG.error("lightblue document: " + response.getJson().toString() + " doesn't equal legacy document: " + node.toString());
				if (shouldOverwriteLightblueDocuments()) {
					overwriteLightblueDocument(node);
				}
				inconsistentDocuments++;
			}
			documentsCompared++;
		}
	}

	private void overwriteLightblueDocument(JsonNode node) {
		LightblueRequest updateRequest = new DataSaveRequest(lightblueEntityName, lightblueEntityVersion);
		updateRequest.setBody(lightblueUpdateJsonExpression.replace("$nodeData", node.toString()));
		LOG.info("lightblue being updated with legacy document document: " + node.toString());
		LightblueResponse response = getClient().data(updateRequest);
		LOG.info("updateResponse: " + response.getText());
		lightblueDocumentsUpdated++;
	}

}
