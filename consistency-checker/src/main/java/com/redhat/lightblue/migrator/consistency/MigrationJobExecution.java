package com.redhat.lightblue.migrator.consistency;

import java.util.Date;

public class MigrationJobExecution {

	private String ownerName;
	private String hostName;
	private String pid;
	
	// actual run times for job
	private Date actualStartDate;
	private Date actualEndDate;

	// did job complete successfully?
	private boolean completedFlag;

	// summary info on what the job did
	private int processedDocumentCount;
	private int consistentDocumentCount;
	private int inconsistentDocumentCount;
	private int overwrittenDocumentCount;
	
	public String getOwnerName() {
		return ownerName;
	}

	public void setOwnerName(String ownerName) {
		this.ownerName = ownerName;
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
		return completedFlag;
	}

	public void setCompletedFlag(boolean completedFlag) {
		this.completedFlag = completedFlag;
	}

	public int getProcessedDocumentCount() {
		return processedDocumentCount;
	}

	public void setProcessedDocumentCount(int processedDocumentCount) {
		this.processedDocumentCount = processedDocumentCount;
	}

	public int getConsistentDocumentCount() {
		return consistentDocumentCount;
	}

	public void setConsistentDocumentCount(int consistentDocumentCount) {
		this.consistentDocumentCount = consistentDocumentCount;
	}

	public int getInconsistentDocumentCount() {
		return inconsistentDocumentCount;
	}

	public void setInconsistentDocumentCount(int inconsistentDocumentCount) {
		this.inconsistentDocumentCount = inconsistentDocumentCount;
	}

	public int getOverwrittenDocumentCount() {
		return overwrittenDocumentCount;
	}

	public void setOverwrittenDocumentCount(int overwrittenDocumentCount) {
		this.overwrittenDocumentCount = overwrittenDocumentCount;
	}
	
}
