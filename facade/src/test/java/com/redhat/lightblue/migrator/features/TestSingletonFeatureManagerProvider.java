package com.redhat.lightblue.migrator.features;

import org.togglz.core.manager.FeatureManager;
import org.togglz.core.manager.FeatureManagerBuilder;
import org.togglz.core.repository.mem.InMemoryStateRepository;
import org.togglz.core.spi.FeatureManagerProvider;

public class TestSingletonFeatureManagerProvider implements FeatureManagerProvider {

    private static FeatureManager featureManager;

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public synchronized FeatureManager getFeatureManager() {
        if (featureManager == null) {
            featureManager = new FeatureManagerBuilder()
                    .featureEnum(LightblueMigrationFeatures.class)
                    .stateRepository(new InMemoryStateRepository())
                    .userProvider(new TogglzRandomUserProvider())
                    .build();
        }

        return featureManager;
    }
}
