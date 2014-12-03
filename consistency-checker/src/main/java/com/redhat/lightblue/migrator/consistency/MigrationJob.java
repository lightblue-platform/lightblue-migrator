package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.expression.query.Query;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class MigrationJob implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(MigrationJob.class);

	public MigrationJob() {

	}

	public MigrationJob(MigrationConfiguration migrationConfiguration) {
		this.migrationConfiguration = migrationConfiguration;
	}

	private LightblueClient sourceClient;
	private LightblueClient destinationClient;

	// configuration for migrator
	private MigrationConfiguration migrationConfiguration;

	// information about migrator instance working job
	private String owner;
	private String hostName;
	private String pid;

	// dates of data to migrate
	private Date startDate;
	private Date endDate;

	// when this job can be worked by migrator
	private Date whenAvailable;

	// how long we think it will take
	private int expectedRunTime;

	// actual run times for job
	private Date actualStartDate;
	private Date actualEndDate;

	// did job complete successfully?
	private boolean completed;

	// summary info on what the job did
	private int documentsProcessed;
	private int consistentDocuments;
	private int inconsistentDocuments;
	private int recordsOverwritten;

	private boolean hasInconsistentDocuments;

	public MigrationConfiguration getJobConfiguration() {
		return migrationConfiguration;
	}

	public void setJobConfiguration(MigrationConfiguration jobConfiguration) {
		this.migrationConfiguration = jobConfiguration;
	}

	public LightblueClient getSourceClient() {
		return sourceClient;
	}

	public void setSourceClient(LightblueClient client) {
		this.sourceClient = client;
	}

	public LightblueClient getDestinationClient() {
		return destinationClient;
	}

	public void setDestinationClient(LightblueClient client) {
		this.destinationClient = client;
	}

	public void setOverwriteDestinationDocuments(boolean overwriteDestinationDocuments) {
		migrationConfiguration.setOverwriteDestinationDocuments(overwriteDestinationDocuments);
	}

	public boolean shouldOverwriteDestinationDocuments() {
		return migrationConfiguration.shouldOverwriteDestinationDocuments();
	}

	/**
	 * Returns true if there are any inconsistent documents
	 * 
	 * @return true if there are any inconsistent documents
	 */
	public boolean hasInconsistentDocuments() {
		return hasInconsistentDocuments;
	}

	public String getConfigurationName() {
		return migrationConfiguration.getConfigurationName();
	}

	public void setConfigurationName(String configurationName) {
		this.migrationConfiguration.setConfigurationName(configurationName);
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public Date getWhenAvailable() {
		return whenAvailable;
	}

	public void setWhenAvailable(Date whenAvailable) {
		this.whenAvailable = whenAvailable;
	}

	public int getExpectedRunTime() {
		return expectedRunTime;
	}

	public void setExpectedRunTime(int expectedRunTime) {
		this.expectedRunTime = expectedRunTime;
	}

	public Date getActualStartDate() {
		return actualStartDate;
	}

	public void setActualStartDate(Date actualStartDate) {
		this.actualStartDate = actualStartDate;
	}

	public Date getActualEndDate() {
		return actualEndDate;
	}

	public void setActualEndDate(Date actualEndDate) {
		this.actualEndDate = actualEndDate;
	}

	public boolean isCompleted() {
		return completed;
	}

	public void setCompleted(boolean completed) {
		this.completed = completed;
	}

	public int getDocumentsProcessed() {
		return documentsProcessed;
	}

	public void setDocumentsProcessed(int documentsProcessed) {
		this.documentsProcessed = documentsProcessed;
	}

	public int getConsistentDocuments() {
		return consistentDocuments;
	}

	public void setConsistentDocuments(int consistentDocuments) {
		this.consistentDocuments = consistentDocuments;
	}

	public int getInconsistentDocuments() {
		return inconsistentDocuments;
	}

	public void setInconsistentDocuments(int inconsistentDocuments) {
		this.inconsistentDocuments = inconsistentDocuments;
	}

	public int getRecordsOverwritten() {
		return recordsOverwritten;
	}

	public void setRecordsOverwritten(int recordsOverwritten) {
		this.recordsOverwritten = recordsOverwritten;
	}

	@Override
	public void run() {
		LOGGER.info("MigrationJob started");

		saveJobDetails();

		configureClients();

		List<JsonNode> sourceDocuments = getSourceDocuments();

		List<JsonNode> destinationDocuments = getDestinationDocuments();

		List<JsonNode> documentsToOverwrite = ListUtils.subtract(sourceDocuments, destinationDocuments);

		if (!documentsToOverwrite.isEmpty()) {
			hasInconsistentDocuments = true;
		}

		documentsProcessed = sourceDocuments.size();
		consistentDocuments = sourceDocuments.size() - documentsToOverwrite.size();
		inconsistentDocuments = documentsToOverwrite.size();

		if (shouldOverwriteDestinationDocuments()) {
			recordsOverwritten = overwriteLightblue(documentsToOverwrite);
		}

		saveJobDetails();

		LOGGER.info("MigrationJob completed");
	}

	private void configureClients() {
		if (migrationConfiguration.getConfigFilePath() == null) {
			sourceClient = new LightblueHttpClient();
			destinationClient = new LightblueHttpClient();
		} else {
			sourceClient = new LightblueHttpClient(migrationConfiguration.getConfigFilePath());
			destinationClient = new LightblueHttpClient(migrationConfiguration.getConfigFilePath());
		}
	}

	private LightblueResponse saveJobDetails() {
		LightblueRequest saveRequest = new DataSaveRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration().getDestinationEntityVersion());
		saveRequest.setBody(this.toJson());
		LightblueResponse response = saveDestinationData(saveRequest);
		return response;
	}

	private int overwriteLightblue(List<JsonNode> documentsToOverwrite) {
		LightblueRequest saveRequest = new DataSaveRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration().getDestinationEntityVersion());
		StringBuffer body = new StringBuffer();
		for (JsonNode document : documentsToOverwrite) {
			body.append(document.toString());
		}
		saveRequest.setBody(body.toString());
		LightblueResponse response = saveDestinationData(saveRequest);
		return response.getJson().findValue("modifiedCount").asInt();
	}

	protected List<JsonNode> getSourceDocuments() {
		List<JsonNode> sourceDocuments = Collections.emptyList();
		try {
			DataFindRequest sourceRequest = new DataFindRequest(getJobConfiguration().getSourceEntityName(), getJobConfiguration().getSourceEntityVersion());
			List<Query> conditions = new LinkedList<Query>();
			conditions.add(withValue(getJobConfiguration().getSourceEntityTimestampField() + " >= " + getStartDate()));
			conditions.add(withValue(getJobConfiguration().getSourceEntityTimestampField() + " <= " + getEndDate()));
			sourceRequest.where(and(conditions));
			sourceRequest.select(includeFieldRecursively("*"));
			sourceDocuments = findSourceData(sourceRequest);
		} catch (IOException e) {
			LOGGER.error("Problem getting sourceDocuments", e);
		}
		return sourceDocuments;
	}

	protected List<JsonNode> getDestinationDocuments() {
		List<JsonNode> destinationDocuments = Collections.emptyList();
		try {
			DataFindRequest destinationRequest = new DataFindRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration()
			    .getDestinationEntityVersion());
			List<Query> conditions = new LinkedList<Query>();
			conditions.add(withValue(getJobConfiguration().getDestinationEntityTimestampField() + " >= " + getStartDate()));
			conditions.add(withValue(getJobConfiguration().getDestinationEntityTimestampField() + " <= " + getEndDate()));
			destinationRequest.where(and(conditions));
			destinationRequest.select(includeFieldRecursively("*"));
			destinationRequest.sort(new SortCondition(getJobConfiguration().getDestinationEntityTimestampField(), SortDirection.ASC));
			destinationDocuments = findDestinationData(destinationRequest);
		} catch (IOException e) {
			LOGGER.error("Error getting destinationDocuments", e);
		}
		return destinationDocuments;
	}

	protected List<JsonNode> findSourceData(LightblueRequest findRequest) throws IOException {
		return Arrays.asList(getSourceClient().data(findRequest, JsonNode[].class));
	}

	protected List<JsonNode> findDestinationData(LightblueRequest findRequest) throws IOException {
		return Arrays.asList(getDestinationClient().data(findRequest, JsonNode[].class));
	}

	protected LightblueResponse saveDestinationData(LightblueRequest saveRequest) {
		return getDestinationClient().data(saveRequest);
	}

	private String toJson() {
		StringBuffer json = new StringBuffer();
		ObjectMapper mapper = new ObjectMapper();
		try {
			json.append(mapper.writeValueAsString(MigrationJob.class));
		} catch (JsonProcessingException e) {
			LOGGER.error("Error transforming to JSON", e);
		}
		return json.toString();
	}

}
