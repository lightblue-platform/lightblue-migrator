package com.redhat.lightblue.migrator.features;

import org.togglz.core.user.FeatureUser;
import org.togglz.core.user.SimpleFeatureUser;
import org.togglz.core.user.UserProvider;

/**
 * Call {@link TogglzRandomUsername#init()} to generate new random username before
 * this provider is used. {@link DAOFacadeBase} apis already handle that.
 *
 * @author mpatercz
 *
 */
public class TogglzRandomUserProvider implements UserProvider {

    @Override
    public FeatureUser getCurrentUser() {
        return new SimpleFeatureUser(TogglzRandomUsername.get(), false);
    }

}
