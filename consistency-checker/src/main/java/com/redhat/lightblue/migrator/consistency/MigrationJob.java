package com.redhat.lightblue.migrator.consistency;

import java.util.Date;
import java.util.List;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public class MigrationJob implements Runnable {
	
	private static final Log LOG = LogFactory.getLog("MigrationJob");
	
	public MigrationJob() {
		
	}
	
	public MigrationJob(JobConfiguration jobConfiguration) {
		this.jobConfiguration = jobConfiguration;
	}

	private LightblueClient client = new LightblueHttpClient();
	
	// configuration for migrator
	private JobConfiguration jobConfiguration;

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
	
	public JobConfiguration getJobConfiguration() {
		return jobConfiguration;
	}

	public void setJobConfiguration(JobConfiguration jobConfiguration) {
		this.jobConfiguration = jobConfiguration;
	}
	
	public LightblueClient getClient() {
		return client;
	}

	public void setClient(LightblueClient client) {
		this.client = client;
	}
	
	public void setOverwriteLightblueDocuments(boolean overwriteLightblueDocuments) {
		jobConfiguration.setOverwriteLightblueDocuments(overwriteLightblueDocuments);
	}
	
	public boolean shouldOverwriteLightblueDocuments() {
		return jobConfiguration.shouldOverwriteLightblueDocuments();
	}
	
	public boolean hasFailures() {
		return hasFailures;
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
		LOG.info("JobInstance started");

		List<JsonNode> legacyDocuments = getLegacyDocuments();
		
		List<JsonNode> lightblueDocuments = getLightblueDocuments(legacyDocuments);
		
		List<JsonNode> documentsToOverwrite = ListUtils.subtract(legacyDocuments, lightblueDocuments); 
    
		if(!documentsToOverwrite.isEmpty()) {
    	hasFailures = true;
    }
		if(shouldOverwriteLightblueDocuments()) {
			overwriteLightblue(documentsToOverwrite);
		}
		
		saveJobDetails();
		
		LOG.info("JobInstance completed");
  }
	
	private LightblueResponse saveJobDetails() {
		DataSaveRequest saveRequest = new DataSaveRequest();
		//TODO Save Request for lightblue
		LightblueResponse response = saveLightblueData(saveRequest);
		return response;
  }

	private LightblueResponse overwriteLightblue(List<JsonNode> documentsToOverwrite) {
		LightblueRequest saveRequest = new DataSaveRequest(getJobConfiguration().getLightblueEntityName(),getJobConfiguration().getLightblueEntityVersion());
		//TODO build the appropriate update statement here, preferably in batch
		LightblueResponse response = saveLightblueData(saveRequest);
		return response;
  }

	protected List<JsonNode> getLegacyDocuments() {
		LightblueRequest legacyRequest = new DataFindRequest();
		//TODO make a batch request here using lightblue using job.getStartDate();job.getEndDate();job.getMigratorConfig().getLightblueEntityTimestampField();
		return getLegacyData(legacyRequest);
	}

	protected List<JsonNode> getLightblueDocuments(List<JsonNode> legacyDocumentsToCompare) {
		LightblueRequest lightblueRequest = new DataFindRequest();
		for(JsonNode node : legacyDocumentsToCompare) {
			for(String keyFieldName : getJobConfiguration().getLegacyEntityKeyFields()) {
				node.findValue(keyFieldName);
				//TODO add to lightblue batch request
			}	
		}	
		return getLightblueData(lightblueRequest);
	}
	
	protected List<JsonNode> getLegacyData(LightblueRequest dataRequest) {
		return getClient().data(dataRequest).getJson().findValues("processed");
	}

	protected List<JsonNode> getLightblueData(LightblueRequest dataRequest) {
		return getClient().data(dataRequest).getJson().findValues("processed");
	}
	
	protected LightblueResponse saveLightblueData(LightblueRequest updateRequest) {
		//return getClient().data(updateRequest);
		return new LightblueResponse();
	}

}
