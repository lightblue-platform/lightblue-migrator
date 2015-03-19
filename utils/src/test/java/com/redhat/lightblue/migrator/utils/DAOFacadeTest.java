package com.redhat.lightblue.migrator.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

@RunWith(MockitoJUnitRunner.class)
public class DAOFacadeTest {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    @Mock CountryDAO legacyDAO;
    @Mock CountryDAO lightblueDAO;
    CountryDAO facade;

    @Before
    public void setup() {
        facade = new DAOFacadeExample(legacyDAO, lightblueDAO);
    }

    /* Read tests */

    @Test
    public void initialPhaseRead() {
        LightblueMigrationPhase.initialPhase(togglzRule);

        facade.getCountry("PL");

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void dualReadPhaseReadConsistentTest() {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country country = new Country();

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(country);
        Mockito.when(lightblueDAO.getCountry("PL")).thenReturn(country);

        facade.getCountry("PL");

        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void dualReadPhaseReadInconsistentTest() {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(ca);
        Mockito.when(lightblueDAO.getCountry("PL")).thenReturn(pl);

        Country returnedCountry = facade.getCountry("PL");

        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, returnedCountry);
    }

    @Test
    public void lightblueProxyTest() {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        facade.getCountry("PL");

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).getCountry("PL");
    }

    /* update tests */

    @Test
    public void initialPhaseUpdate() {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        facade.updateCountry(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).updateCountry(pl);
    }

    @Test
    public void dualWritePhaseUpdateConsistentTest() {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");

        facade.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
    }

    @Test
    public void dualWritePhaseUpdateInconsistentTest() {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.updateCountry(pl)).thenReturn(ca);
        Mockito.when(lightblueDAO.updateCountry(pl)).thenReturn(pl);

        Country updatedEntity = facade.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, updatedEntity);
    }

    @Test
    public void ligtblueProxyPhaseUpdateTest() {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        facade.updateCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).updateCountry(pl);
    }

    /* insert tests */

    // TODO: implement insert tests

}
