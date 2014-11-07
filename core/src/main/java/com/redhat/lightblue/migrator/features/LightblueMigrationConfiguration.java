package com.redhat.lightblue.migrator.features;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import javax.enterprise.context.ApplicationScoped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.togglz.core.Feature;
import org.togglz.core.manager.TogglzConfig;
import org.togglz.core.repository.StateRepository;
import org.togglz.core.repository.file.FileBasedStateRepository;
import org.togglz.core.user.UserProvider;
import org.togglz.servlet.user.ServletUserProvider;

@ApplicationScoped
public class LightblueMigrationConfiguration implements TogglzConfig {

	private String configFilePath = "lightblue-featureflags.properties";

	private static final Logger LOGGER = LoggerFactory.getLogger(LightblueMigrationConfiguration.class);

	public String getConfigFilePath() {
		return configFilePath;
	}

	public void setConfigFilePath(String configFilePath) {
		this.configFilePath = configFilePath;
	}

	private String getStateRepositoryFilePath() {
		try {
			Properties properties = new Properties();
			properties.load(getClass().getClassLoader().getResourceAsStream(getConfigFilePath()));
			return properties.getProperty("stateRepositoryPath");
		} catch (IOException io) {
			LOGGER.error(getConfigFilePath() + " could not be found/read", io);
			throw new RuntimeException(io);
		}
	}

	@Override
	public Class<? extends Feature> getFeatureClass() {
		return LightblueMigrationFeatures.class;
	}

	@Override
	public StateRepository getStateRepository() {
		return new FileBasedStateRepository(new File(getStateRepositoryFilePath()));

	}

	public UserProvider getUserProvider() {
		return new ServletUserProvider(getMigrationAdminGroup());
	}

	private String getMigrationAdminGroup() {
		try {
			Properties properties = new Properties();
			properties.load(getClass().getClassLoader().getResourceAsStream(getConfigFilePath()));
			return properties.getProperty("migrationAdminRole");
		} catch (IOException io) {
			LOGGER.error(getConfigFilePath() + " could not be found/read", io);
			throw new RuntimeException(io);
		}
	}

}
