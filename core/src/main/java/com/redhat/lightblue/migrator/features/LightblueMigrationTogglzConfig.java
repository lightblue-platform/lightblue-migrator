package com.redhat.lightblue.migrator.features;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.togglz.core.Feature;
import org.togglz.core.manager.TogglzConfig;
import org.togglz.core.repository.StateRepository;
import org.togglz.core.repository.util.DefaultMapSerializer;
import org.togglz.core.user.UserProvider;

/**
 * TogglzConfig configured for lightblue migration needs: @{link {@link LightblueMigrationFeatures}, database state repository and
 * randomized usernames (see {@link TogglzRandomUsername}).
 *
 * @author mpatercz
 *
 */
@ApplicationScoped
public class LightblueMigrationTogglzConfig implements TogglzConfig {

    Logger logger = LoggerFactory.getLogger(LightblueMigrationTogglzConfig.class);

    private LightblueMigrationStateRepositoryProvider stateRepositoryProvider;

    public LightblueMigrationTogglzConfig() {
        logger.info("LightblueMigrationTogglzConfig initialized");
    }

    @Inject
    public LightblueMigrationTogglzConfig(LightblueMigrationStateRepositoryProvider stateRepositoryProvider) throws IOException, NamingException {
        logger.info("LightblueMigrationTogglzConfig initialized with stateRepositoryProvider");
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

    public static void main(String[] args) {
        DefaultMapSerializer s = DefaultMapSerializer.singleline();

        Map<String,String> params = new HashMap<String,String>();
        params.put("param", "value");

        String str = s.serialize(params);

        System.out.println(str);

        System.out.println(s.deserialize(str).get("param"));
    }

}
