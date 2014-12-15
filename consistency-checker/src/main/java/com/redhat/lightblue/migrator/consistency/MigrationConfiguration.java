package com.redhat.lightblue.migrator.consistency;

import java.util.List;

public class MigrationConfiguration {

	private String configFilePath;

	private String hostName;
	private int threadCount;

	private String configurationName;
	private String consistencyCheckerName;

	private List<String> comparisonExclusionPaths;
	
	private String destinationEntityName;
	private String destinationEntityVersion;
	private List<String> destinationEntityKeyFields;

	private String sourceEntityName;
	private String sourceEntityVersion;
	private String sourceTimestampPath;

	private boolean overwriteDestinationDocuments = false;

	public String getConfigFilePath() {
		return configFilePath;
	}

	public void setConfigFilePath(String configFilePath) {
		this.configFilePath = configFilePath;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getThreadCount() {
		return threadCount;
	}

	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}

	public String getConfigurationName() {
		return configurationName;
	}

	public void setConfigurationName(String configurationName) {
		this.configurationName = configurationName;
	}
	
	public void setConsistencyCheckerName(String name) {
		this.consistencyCheckerName = name;
	}

	public String getConsistencyCheckerName() {
		return consistencyCheckerName;
	}

	public List<String> getComparisonExclusionPaths() {
		return comparisonExclusionPaths;
	}

	public void setComparisonExclusionPaths(List<String> comparisonExclusionPaths) {
		this.comparisonExclusionPaths = comparisonExclusionPaths;
	}
	
	public String getDestinationEntityName() {
		return destinationEntityName;
	}
	
	public void setDestinationEntityName(String destinationEntityName) {
		this.destinationEntityName = destinationEntityName;
	}

	public String getDestinationEntityVersion() {
		return destinationEntityVersion;
	}

	public void setDestinationEntityVersion(String destinationEntityVersion) {
		this.destinationEntityVersion = destinationEntityVersion;
	}

	public List<String> getDestinationEntityKeyFields() {
		return destinationEntityKeyFields;
	}

	public void setDestinationEntityKeyFields(List<String> destinationEntityKeyFields) {
		this.destinationEntityKeyFields = destinationEntityKeyFields;
	}

	public String getSourceEntityName() {
		return sourceEntityName;
	}

	public void setSourceEntityName(String sourceEntityName) {
		this.sourceEntityName = sourceEntityName;
	}

	public String getSourceEntityVersion() {
		return sourceEntityVersion;
	}

	public void setSourceEntityVersion(String sourceEntityVersion) {
		this.sourceEntityVersion = sourceEntityVersion;
	}

	public String getSourceTimestampPath() {
		return sourceTimestampPath;
	}

	public void setSourceTimestampPath(String sourceTimestampPath) {
		this.sourceTimestampPath = sourceTimestampPath;
	}

	public void setOverwriteDestinationDocuments(boolean overwriteDestinationDocuments) {
		this.overwriteDestinationDocuments = overwriteDestinationDocuments;
	}

	public boolean shouldOverwriteDestinationDocuments() {
		return overwriteDestinationDocuments;
	}

}
