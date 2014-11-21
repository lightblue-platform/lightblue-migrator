package com.redhat.lightblue.migrator.consistency;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;

public class ConsistencyChecker {

	LightblueClient client = new LightblueHttpClient();

	public static final Log LOG = LogFactory.getLog("CompareLightblueToLegacyCommand");

	private String checkerName;
	private String hostname;
	private String ipAddress;
	private String serviceURI;

	private boolean hasFailures = false;

	public boolean hasFailures() {
		return hasFailures;
	}

	public String getCheckerName() {
		return checkerName;
	}

	public void setCheckerName(String name) {
		this.checkerName = name;
	}

	public void setClient(LightblueClient client) {
		this.client = client;
	}

	
	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getServiceURI() {
		return serviceURI;
	}

	public void setServiceURI(String serviceURI) {
		this.serviceURI = serviceURI;
	}

	public void execute() throws InterruptedException {
		List<ExecutorService> executors = new ArrayList<>();

		LOG.info("From CLI - name: " + getCheckerName() + " hostname: " + getHostname() + " ipAddress: " + getIpAddress());
		// client = LightblueHttpClient(String dataServiceURI, String
		// metadataServiceURI, false);
		// TODO make client usage less painful
		while (true) {
			List<JobConfiguration> configurations = getJobConfigurations(checkerName);

			for (JobConfiguration configuration : configurations) {
				ExecutorService jobExecutor = Executors.newFixedThreadPool(configuration.getThreadCount());
				executors.add(jobExecutor);
				List<MigrationJob> jobs = getJobs(configuration);
				for (int i = 0; i < jobs.size(); i++) {
					MigrationJob migrationJob = new MigrationJob();
					jobExecutor.execute(migrationJob);
				}
			}
			for (ExecutorService executor : executors) {
				executor.shutdown();
			}			
			
//			if()
			//Thread.sleep(10);
		}

	}

	protected List<MigrationJob> getJobs(JobConfiguration configuration) {
		ArrayList<MigrationJob> jobs = new ArrayList<>();
		// get jobs from lightblue for this configuration
		DataFindRequest findRequest = new DataFindRequest();
		// TODO set up stuff to find by checkerName
		//client.data(findRequest);
				
		return jobs;
	}

	protected List<JobConfiguration> getJobConfigurations(String checkerName) {
		ArrayList<JobConfiguration> configurations = new ArrayList<>();
		// get job configurations from lightblue for this instance of consistency
		// checker
		DataFindRequest findRequest = new DataFindRequest();
		// TODO set up stuff to find by checkerName
		//client.data(findRequest);
		
		return configurations;
	}

}
