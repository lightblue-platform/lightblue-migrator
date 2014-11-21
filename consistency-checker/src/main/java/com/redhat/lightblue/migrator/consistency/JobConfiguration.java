package com.redhat.lightblue.migrator.consistency;

import java.util.List;

public class JobConfiguration {

	private String configFilePath;

	private String hostName;
	private int threadCount;

	private String lightblueEntityName;
	private String lightblueEntityVersion;
	private List<String> lightblueEntityKeyFields;
	private String lightblueEntityTimestampField;

	private String legacyEntityName;
	private String legacyEntityVersion;
	private List<String> legacyEntityKeyFields;
	private String legacyEntityTimestampField;

	private boolean overwriteLightblueDocuments = false;

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

	public List<String> getLightblueEntityKeyFields() {
		return lightblueEntityKeyFields;
	}

	public void setLightblueEntityKeyFields(List<String> lightblueEntityKeyFields) {
		this.lightblueEntityKeyFields = lightblueEntityKeyFields;
	}

	public String getLightblueEntityTimestampField() {
		return lightblueEntityTimestampField;
	}

	public void setLightblueEntityTimestampField(String lightblueEntityTimestampField) {
		this.lightblueEntityTimestampField = lightblueEntityTimestampField;
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

	public List<String> getLegacyEntityKeyFields() {
		return legacyEntityKeyFields;
	}

	public void setLegacyEntityKeyFields(List<String> legacyEntityKeyFields) {
		this.legacyEntityKeyFields = legacyEntityKeyFields;
	}

	public String getLegacyEntityTimestampField() {
		return legacyEntityTimestampField;
	}

	public void setLegacyEntityTimestampField(String legacyEntityTimestampField) {
		this.legacyEntityTimestampField = legacyEntityTimestampField;
	}

	public void setOverwriteLightblueDocuments(boolean overwriteLightblueDocuments) {
		this.overwriteLightblueDocuments = overwriteLightblueDocuments;
	}

	public boolean shouldOverwriteLightblueDocuments() {
		return overwriteLightblueDocuments;
	}

}
