package com.redhat.lightblue.migrator.consistency;

import java.util.Date;

public class MigrationJobRun {

	private String owner;
	private String hostName;
	private String pid;
	
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
	
}
