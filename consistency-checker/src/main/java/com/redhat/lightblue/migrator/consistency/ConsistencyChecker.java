package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.util.ClientConstants;

public class ConsistencyChecker {

	LightblueClient client;

	public static final Logger LOGGER = LoggerFactory.getLogger(MigrationJob.class);

	public static final int MAX_WAIT_TIME = 86400000; // 24 hours

	private String name;
	private String hostName;
	private String ipAddress;
	private String configPath;
	private String migrationConfigurationEntityVersion;
	private String migrationJobEntityVersion;

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

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
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

	public String getMigrationConfigurationEntityVersion() {
		return migrationConfigurationEntityVersion;
	}

	public void setMigrationConfigurationEntityVersion(String migrationConfigurationEntityVersion) {
		this.migrationConfigurationEntityVersion = migrationConfigurationEntityVersion;
	}

	public String getMigrationJobEntityVersion() {
		return migrationJobEntityVersion;
	}

	public void setMigrationJobEntityVersion(String migrationJobEntityVersion) {
		this.migrationJobEntityVersion = migrationJobEntityVersion;
	}

	public void execute() throws Exception {

		LOGGER.info("From CLI - name: " + getName() + " hostName: " + getHostName() + " ipAddress: " + getIpAddress());

		if (configPath != null) {
			client = new LightblueHttpClient(configPath);
		} else {
			client = new LightblueHttpClient();
		}

		while (run) {
			List<ExecutorService> executors = new ArrayList<>();
			List<MigrationConfiguration> configurations = getJobConfigurations();

			for (MigrationConfiguration configuration : configurations) {
				configuration.setConfigFilePath(configPath);
				List<MigrationJob> jobs = getMigrationJobs(configuration);

				if (!jobs.isEmpty()) {
					ExecutorService jobExecutor = Executors.newFixedThreadPool(configuration.getThreadCount());
					executors.add(jobExecutor);
					for (MigrationJob job : jobs) {
						job.setHostName(getHostName());
						jobExecutor.execute(job);
					}
				}
			}

			if (executors.isEmpty()) {
				MigrationJob nextJob = getNextAvailableJob();
				long timeUntilNextJob = nextJob.getWhenAvailable().getTime() - new Date().getTime();
				Thread.sleep((timeUntilNextJob > MAX_WAIT_TIME) ? MAX_WAIT_TIME : timeUntilNextJob);
			} else {
				for (ExecutorService executor : executors) {
					executor.shutdown();
					executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				}
			}
		}
	}

	protected List<MigrationJob> getMigrationJobs(MigrationConfiguration configuration) {
		List<MigrationJob> jobs = Collections.emptyList();
		try {
			DataFindRequest findRequest = new DataFindRequest("migrationJob", migrationJobEntityVersion);
			findRequest.where(withValue("name = " + configuration.getName()));
			findRequest.select(includeFieldRecursively("*"));
			jobs.addAll(Arrays.asList(client.data(findRequest, MigrationJob[].class)));
		} catch (IOException e) {
			LOGGER.error("Problem getting migrationJobs", e);
		}
		return jobs;
	}

	protected List<MigrationConfiguration> getJobConfigurations() {
		List<MigrationConfiguration> configurations = Collections.emptyList();
		try {
			DataFindRequest findRequest = new DataFindRequest("migrationConfiguration", migrationConfigurationEntityVersion);
			findRequest.where(withValue("name = " + getName()));
			findRequest.select(includeFieldRecursively("*"));
			configurations.addAll(Arrays.asList(client.data(findRequest, MigrationConfiguration[].class)));
		} catch (IOException e) {
			LOGGER.error("Problem getting migrationConfigurations", e);
		}
		return configurations;
	}

	protected MigrationJob getNextAvailableJob() {
		MigrationJob job = null;
		try {
			DataFindRequest findRequest = new DataFindRequest("migrationJob", migrationJobEntityVersion);
			findRequest.where(withValue("whenAvailable >= " + ClientConstants.getDateFormat().format(new Date())));
			findRequest.sort(new SortCondition("whenAvailable", SortDirection.ASC));
			findRequest.range(0, 1);
			findRequest.select(includeFieldRecursively("*"));
			job = client.data(findRequest, MigrationJob.class);
		} catch (IOException e) {
			LOGGER.error("Problem getting migrationJob", e);
		}
		return job;
	}

}
