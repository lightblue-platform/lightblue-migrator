package com.redhat.lightblue.migrator.facade;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

@RunWith(MockitoJUnitRunner.class)
public abstract class ServiceFacadeTestBase {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    CountryDAOFacadable legacyDAO = Mockito.mock(CountryDAOFacadable.class);
    CountryDAOFacadable lightblueDAO = Mockito.mock(CountryDAOFacadable.class);
    CountryDAO countryDAOProxy;

    @Spy
    ServiceFacade<CountryDAOFacadable> daoFacade = new ServiceFacade<CountryDAOFacadable>(legacyDAO, lightblueDAO, "CountryDAOFacade");
    @Spy
    ConsistencyChecker consistencyChecker = new ConsistencyChecker(CountryDAO.class.getSimpleName());

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        daoFacade.setConsistencyChecker(consistencyChecker);

        // countryDAOProxy is daoFacade using CountryDAO interface to invoke methods
        countryDAOProxy = FacadeProxyFactory.createFacadeProxy(daoFacade, CountryDAOFacadable.class);

        Mockito.verify(legacyDAO).setSharedStore((daoFacade).getSharedStore());
        Mockito.verify(lightblueDAO).setSharedStore((daoFacade).getSharedStore());

        LightblueMigrationPhase.initialPhase(togglzRule);
    }

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verifyNoMoreInteractions(legacyDAO);
    }

}
