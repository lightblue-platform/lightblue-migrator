package com.redhat.lightblue.migrator.utils.togglz;

import org.togglz.core.user.FeatureUser;
import org.togglz.core.user.SimpleFeatureUser;
import org.togglz.core.user.UserProvider;

import com.redhat.lightblue.migrator.utils.DAOFacadeBase;

/**
 * Call {@link TogglzRandomUsername#init()} to generate new random username before
 * the provider is used. {@link DAOFacadeBase} apis already handle that.
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
