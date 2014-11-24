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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.util.ClientConstants;

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

		if (configPath != null) {
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
		List<MigrationJob> jobs = Collections.emptyList();
		try {
			DataFindRequest findRequest = new DataFindRequest("migrationJob", "0.1.0-SNAPSHOT");
			findRequest.where(withValue("name = " + configuration.getName()));
			findRequest.select(includeFieldRecursively("*"));
			jobs.addAll(Arrays.asList(client.data(findRequest, MigrationJob[].class)));
		} catch (IOException e) {
			LOG.error("Problem getting migrationJobs", e);
		}
		return jobs;
	}

	protected List<MigrationConfiguration> getJobConfigurations(String checkerName) {
		List<MigrationConfiguration> configurations = Collections.emptyList();
		try {
			DataFindRequest findRequest = new DataFindRequest("migrationConfiguration", "0.1.0-SNAPSHOT");
			findRequest.where(withValue("name = " + getName()));
			findRequest.select(includeFieldRecursively("*"));
			configurations.addAll(Arrays.asList(client.data(findRequest, MigrationConfiguration[].class)));
		} catch (IOException e) {
			LOG.error("Problem getting migrationConfigurations", e);
		}
		return configurations;
	}

	protected MigrationJob getNextAvailableJob() {
		MigrationJob job = null;
		try {
			DataFindRequest findRequest = new DataFindRequest("migrationJob", "0.1.0-SNAPSHOT");
			findRequest.where(withValue("whenAvailable >= " + ClientConstants.getDateFormat().format(new Date())));
			findRequest.sort(new SortCondition("whenAvailable", SortDirection.ASC));
			findRequest.range(0, 0);
			findRequest.select(includeFieldRecursively("*"));
			job = client.data(findRequest, MigrationJob.class);
		} catch (IOException e) {
			LOG.error("Problem getting migrationJob", e);
		}
		return job;
	}

}
