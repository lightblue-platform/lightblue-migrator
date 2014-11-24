package com.redhat.lightblue.migrator.consistency;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;

public class ConsistencyChecker {

	LightblueClient client;

	public static final Log LOG = LogFactory.getLog("ConsistencyChecker");

	private String name;
	private String hostname;
	private String ipAddress;
	private String configPath;

	private boolean run = true;

	public void setRun(boolean run) {
		this.run = run;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public LightblueClient getClient() {
		return client;
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

	public String getConfigPath() {
		return configPath;
	}

	public void setConfigPath(String configPath) {
		this.configPath = configPath;
	}

	public void execute() throws Exception {

		LOG.info("From CLI - name: " + getName() + " hostname: " + getHostname() + " ipAddress: " + getIpAddress());

		if(configPath != null) {
			client = new LightblueHttpClient(configPath);	
		} else {
			client = new LightblueHttpClient();
		}
		
		while (run) {
			List<ExecutorService> executors = new ArrayList<>();
			List<MigrationConfiguration> configurations = getJobConfigurations(name);

			for (MigrationConfiguration configuration : configurations) {
				configuration.setConfigFilePath(configPath);
				List<MigrationJob> jobs = getMigrationJobs(configuration);

				if (!jobs.isEmpty()) {
					ExecutorService jobExecutor = Executors.newFixedThreadPool(configuration.getThreadCount());
					executors.add(jobExecutor);
					for (MigrationJob job : jobs) {
						jobExecutor.execute(job);
					}
				}
			}

			if (executors.isEmpty()) {
				MigrationJob nextJob = getNextAvailableJob();
				long timeToWait = nextJob.getWhenAvailable().getTime() - new Date().getTime();
				Thread.sleep(timeToWait);
			} else {
				for (ExecutorService executor : executors) {
					executor.shutdown();
				}
			}
		}

	}

	protected List<MigrationJob> getMigrationJobs(MigrationConfiguration configuration) {
		ArrayList<MigrationJob> jobs = new ArrayList<>();
		DataFindRequest findRequest = new DataFindRequest();
		// TODO set up stuff to find by checkerName
		getClient().data(findRequest);
		// TODO convert response into MigrationJob
		return jobs;
	}

	protected List<MigrationConfiguration> getJobConfigurations(String checkerName) {
		ArrayList<MigrationConfiguration> configurations = new ArrayList<>();
		// get job configurations from lightblue for this instance of consistency
		// checker
		DataFindRequest findRequest = new DataFindRequest();
		// TODO set up stuff to find by checkerName
		getClient().data(findRequest);
		// TODO convert response into JobConfiguration
		return configurations;
	}

	protected MigrationJob getNextAvailableJob() {
		MigrationJob job = new MigrationJob();
		DataFindRequest findRequest = new DataFindRequest();
		// TODO populate request here
		getClient().data(findRequest);
		return job;
	}

}
