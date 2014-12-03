package com.redhat.lightblue.migrator.consistency;

import java.util.List;

public class MigrationConfiguration {

    private String configFilePath;

    private String hostName;
    private int threadCount;

    private String name;

    private String destinationEntityName;
    private String destinationEntityVersion;
    private List<String> destinationEntityKeyFields;
    private String destinationEntityTimestampField;

    private String sourceEntityName;
    private String sourceEntityVersion;
    private List<String> sourceEntityKeyFields;
    private String sourceEntityTimestampField;

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

    public String getDestinationEntityName() {
        return destinationEntityName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
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

    public String getDestinationEntityTimestampField() {
        return destinationEntityTimestampField;
    }

    public void setDestinationEntityTimestampField(String destinationEntityTimestampField) {
        this.destinationEntityTimestampField = destinationEntityTimestampField;
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

    public List<String> getSourceEntityKeyFields() {
        return sourceEntityKeyFields;
    }

    public void setSourceEntityKeyFields(List<String> sourceEntityKeyFields) {
        this.sourceEntityKeyFields = sourceEntityKeyFields;
    }

    public String getSourceEntityTimestampField() {
        return sourceEntityTimestampField;
    }

    public void setSourceEntityTimestampField(String sourceEntityTimestampField) {
        this.sourceEntityTimestampField = sourceEntityTimestampField;
    }

    public void setOverwriteDestinationDocuments(boolean overwriteDestinationDocuments) {
        this.overwriteDestinationDocuments = overwriteDestinationDocuments;
    }

    public boolean shouldOverwriteDestinationDocuments() {
        return overwriteDestinationDocuments;
    }

}
