package com.redhat.lightblue.migrator.features;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.togglz.core.Feature;
import org.togglz.core.manager.TogglzConfig;
import org.togglz.core.repository.StateRepository;
import org.togglz.core.user.UserProvider;

@ApplicationScoped
public class LightblueMigrationTogglzConfig implements TogglzConfig {

    Logger logger = LoggerFactory.getLogger(LightblueMigrationTogglzConfig.class);

    private LightblueMigrationStateRepositoryProvider stateRepositoryProvider;

    @Inject
    public LightblueMigrationTogglzConfig(LightblueMigrationStateRepositoryProvider stateRepositoryProvider) throws IOException, NamingException {
        this.stateRepositoryProvider = stateRepositoryProvider;
    }

    @Override
    public Class<? extends Feature> getFeatureClass() {
        return LightblueMigrationFeatures.class;
    }

    @Override
    public StateRepository getStateRepository() {
        try {
            return stateRepositoryProvider.getStateRepository();
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public UserProvider getUserProvider() {
        return new TogglzRandomUserProvider();
    }

}
