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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	private static final Log LOG = LogFactory.getLog("MigrationJob");

	public MigrationJob() {

	}

	public MigrationJob(MigrationConfiguration migrationConfiguration) {
		this.migrationConfiguration = migrationConfiguration;
	}

	private LightblueClient legacyClient;
	private LightblueClient lightblueClient;

	// configuration for migrator
	private MigrationConfiguration migrationConfiguration;

	private String name;

	// information about migrator instance working job
	private String owner;
	private String hostname;
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

	private boolean hasFailures;

	public MigrationConfiguration getJobConfiguration() {
		return migrationConfiguration;
	}

	public void setJobConfiguration(MigrationConfiguration jobConfiguration) {
		this.migrationConfiguration = jobConfiguration;
	}

	public LightblueClient getLegacyClient() {
		return legacyClient;
	}

	public void setLegacyClient(LightblueClient client) {
		this.legacyClient = client;
	}

	public LightblueClient getLightblueClient() {
		return lightblueClient;
	}

	public void setLightblueClient(LightblueClient client) {
		this.lightblueClient = client;
	}

	public void setOverwriteLightblueDocuments(boolean overwriteLightblueDocuments) {
		migrationConfiguration.setOverwriteLightblueDocuments(overwriteLightblueDocuments);
	}

	public boolean shouldOverwriteLightblueDocuments() {
		return migrationConfiguration.shouldOverwriteLightblueDocuments();
	}

	public boolean hasFailures() {
		return hasFailures;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
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
		LOG.info("MigrationJob started");

		configureClients();

		List<JsonNode> legacyDocuments = getLegacyDocuments();

		List<JsonNode> lightblueDocuments = getLightblueDocuments();

		List<JsonNode> documentsToOverwrite = ListUtils.subtract(legacyDocuments, lightblueDocuments);

		if (!documentsToOverwrite.isEmpty()) {
			hasFailures = true;
		}
		if (shouldOverwriteLightblueDocuments()) {
			overwriteLightblue(documentsToOverwrite);
		}

		saveJobDetails();

		LOG.info("MigrationJob completed");
	}

	private void configureClients() {
		if (migrationConfiguration.getConfigFilePath() == null) {
			legacyClient = new LightblueHttpClient();
			lightblueClient = new LightblueHttpClient();
		} else {
			legacyClient = new LightblueHttpClient(migrationConfiguration.getConfigFilePath());
			lightblueClient = new LightblueHttpClient(migrationConfiguration.getConfigFilePath());
		}
	}

	private LightblueResponse saveJobDetails() {
		LightblueRequest saveRequest = new DataSaveRequest(getJobConfiguration().getLightblueEntityName(), getJobConfiguration().getLightblueEntityVersion());
		saveRequest.setBody(this.toJson());
		LightblueResponse response = saveLightblueData(saveRequest);
		return response;
	}

	private LightblueResponse overwriteLightblue(List<JsonNode> documentsToOverwrite) {
		LightblueRequest saveRequest = new DataSaveRequest(getJobConfiguration().getLightblueEntityName(), getJobConfiguration().getLightblueEntityVersion());
		StringBuffer body = new StringBuffer();
		for (JsonNode document : documentsToOverwrite) {
			body.append(document.toString());
		}
		saveRequest.setBody(body.toString());
		LightblueResponse response = saveLightblueData(saveRequest);
		return response;
	}

	protected List<JsonNode> getLegacyDocuments() {
		List<JsonNode> legacyDocuments = Collections.emptyList();
		try {
			DataFindRequest legacyRequest = new DataFindRequest(getJobConfiguration().getLegacyEntityName(), getJobConfiguration().getLegacyEntityVersion());
			List<Query> conditions = new LinkedList<Query>();
			conditions.add(withValue(getJobConfiguration().getLegacyEntityTimestampField() + " >= " + getStartDate()));
			conditions.add(withValue(getJobConfiguration().getLegacyEntityTimestampField() + " <= " + getEndDate()));
			legacyRequest.where(and(conditions));
			legacyRequest.select(includeFieldRecursively("*"));
			legacyDocuments = findLegacyData(legacyRequest);
		} catch (IOException e) {
			LOG.error("Problem getting legacyDocuments", e);
		}
		return legacyDocuments;
	}

	protected List<JsonNode> getLightblueDocuments() {
		List<JsonNode> lightblueDocuments = Collections.emptyList();
		try {
			DataFindRequest lightblueRequest = new DataFindRequest(getJobConfiguration().getLightblueEntityName(), getJobConfiguration().getLightblueEntityVersion());
			List<Query> conditions = new LinkedList<Query>();
			conditions.add(withValue(getJobConfiguration().getLightblueEntityTimestampField() + " >= " + getStartDate()));
			conditions.add(withValue(getJobConfiguration().getLightblueEntityTimestampField() + " <= " + getEndDate()));
			lightblueRequest.where(and(conditions));
			lightblueRequest.select(includeFieldRecursively("*"));
			lightblueRequest.sort(new SortCondition(getJobConfiguration().getLightblueEntityTimestampField(), SortDirection.ASC));
			lightblueDocuments = findLightblueData(lightblueRequest);
		} catch (IOException e) {
			LOG.error("Error getting lightblueDocuments", e);
		}
		return lightblueDocuments;
	}

	protected List<JsonNode> findLegacyData(LightblueRequest findRequest) throws IOException {
		return Arrays.asList(getLegacyClient().data(findRequest, JsonNode[].class));
	}

	protected List<JsonNode> findLightblueData(LightblueRequest findRequest) throws IOException {
		return Arrays.asList(getLightblueClient().data(findRequest, JsonNode[].class));
	}

	protected LightblueResponse saveLightblueData(LightblueRequest saveRequest) {
		return getLightblueClient().data(saveRequest);
	}

	private String toJson() {
		StringBuffer json = new StringBuffer();
		ObjectMapper mapper = new ObjectMapper();
		try {
			json.append(mapper.writeValueAsString(MigrationJob.class));
		} catch (JsonProcessingException e) {
			LOG.error("Error transforming to JSON", e);
		}
		return json.toString();
	}

}
