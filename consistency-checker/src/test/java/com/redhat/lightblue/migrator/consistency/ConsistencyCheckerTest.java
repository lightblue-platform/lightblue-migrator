package com.redhat.lightblue.migrator.consistency;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;

public class ConsistencyCheckerTest {

	private String checkerName = "testChecker";
	private String hostname = "http://lightblue.io";
	private String ipAddress = "127.0.0.1";
	private String configPath = "lightblue-client.properties";

	ConsistencyChecker checker;
	
	LightblueClient client;
	
	@Before
	public void setUp() throws Exception {
		checker = new ConsistencyChecker();
		checker.setName(checkerName);
		checker.setHostname(hostname);
		checker.setIpAddress(ipAddress);
		checker.setConfigPath(configPath);
		client = new LightblueHttpClient();
		checker.setClient(client);
	}

	@Test
	public void testGetCheckerName() {
		Assert.assertEquals(checkerName, checker.getName());
	}

	@Test
	public void testSetCheckerName() {
		checker.setName(hostname);
		Assert.assertEquals(hostname, checker.getName());
	}

	@Test
	public void testGetHostName() {
		Assert.assertEquals(hostname, checker.getHostname());
	}

	@Test
	public void testSetHostName() {
		checker.setHostname(checkerName);
		Assert.assertEquals(checkerName, checker.getHostname());
	}

	@Test
	public void testGetIpAddress() {
		Assert.assertEquals(ipAddress, checker.getIpAddress());
	}

	@Test
	public void testSetIpAddress() {
		checker.setIpAddress(hostname);
		Assert.assertEquals(hostname, checker.getIpAddress());
	}
	
	@Test
	public void testGetServiceURI() {
		Assert.assertEquals(configPath, checker.getConfigPath());
	}

	@Test
	public void testSetServiceURI() {
		checker.setConfigPath(ipAddress);
		Assert.assertEquals(ipAddress, checker.getConfigPath());
	}
	
	@Test
	public void testExecute() throws Exception {
		ConsistencyChecker checker = new ConsistencyChecker() {
			public int numRuns = 0;

			@Override
			protected List<MigrationJob> getMigrationJobs(MigrationConfiguration configuration) {
				ArrayList<MigrationJob> jobs = new ArrayList<>();
				if(numRuns == 0 || numRuns == 2) {
					for(int i=0;i<10;i++) {
						MigrationJob job = new MigrationJob() {
							@Override
							public void run() {
								LOG.info("MigrationJob started");
								LOG.info("MigrationJob completed");
							}
						};
						jobs.add(job);	
					}	
				}
				return jobs;
			}
			
			@Override
			protected List<MigrationConfiguration> getJobConfigurations(String checkerName) {
				ArrayList<MigrationConfiguration> configurations = new ArrayList<>();
				;
				for(int i=0;i<5;i++) {
					MigrationConfiguration config = new MigrationConfiguration();
					config.setThreadCount(5);
					configurations.add(config);	
				}
				
				numRuns++;
				if(numRuns > 2){
					setRun(false);
				}
				return configurations;
			}

			protected MigrationJob getNextAvailableJob() {
				MigrationJob job = new MigrationJob();
				job.setWhenAvailable(DateUtils.addSeconds(new Date(), 5));
				return job;
			}

		};
		checker.run();

	}
	
}
