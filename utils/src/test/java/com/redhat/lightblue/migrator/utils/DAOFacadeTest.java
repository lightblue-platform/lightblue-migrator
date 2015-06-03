package com.redhat.lightblue.migrator.utils;

import org.junit.After;
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
    @Mock CountryDAOLightblue lightblueDAO;
    CountryDAO facade;

    @Before
    public void setup() {
        facade = new DAOFacadeExample(legacyDAO, lightblueDAO);
        Mockito.verify(lightblueDAO).setEntityIdStore(((DAOFacadeBase)facade).getEntityIdStore());
        //((DAOFacadeBase)facade).setEntityIdStore(entityIdStore);
    }

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verifyNoMoreInteractions(legacyDAO);
    }

    /* Read tests */

    @Test
    public void initialPhaseRead() {
        LightblueMigrationPhase.initialPhase(togglzRule);

        facade.getCountry("PL");

        Mockito.verifyNoMoreInteractions(lightblueDAO);
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
        Mockito.verify(lightblueDAO).getCountry("PL");
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
        Mockito.verify(lightblueDAO).getCountry("PL");

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

        Mockito.verifyNoMoreInteractions(lightblueDAO);
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

    @Test
    public void initialPhaseCreate() {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        facade.createCountry(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountry(pl);
    }

    @Test
    public void dualWritePhaseCreateConsistentTest() {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);

        Country createdCountry = facade.createCountry(pl);
        Assert.assertEquals(101l, createdCountry.getId());


        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == (Long)((DAOFacadeBase)facade).getEntityIdStore().pop());
    }

    @Test
    public void dualWritePhaseCreateInconsistentTest() {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);
        Mockito.when(lightblueDAO.createCountry(pl)).thenReturn(pl);

        Country createdCountry = facade.createCountry(pl);

        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == (Long) ((DAOFacadeBase) facade).getEntityIdStore().pop());

        Assert.assertEquals(pl.getIso2Code(), "PL");
    }

    @Test
    public void ligtblueProxyPhaseCreateTest() {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        // lightblue will handle ID generation in this phase
        ((DAOFacadeBase) facade).setEntityIdStore(null);
        Mockito.verify(lightblueDAO).setEntityIdStore(null);

        Country pl = new Country("PL");

        facade.createCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountry(pl);
    }

    /* insert tests when method also does a read */

    @Test
    public void initialPhaseCreateWithRead() {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        facade.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
    }

    @Test
    public void dualWritePhaseCreateWithReadTest() {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountryIfNotExists(pl)).thenReturn(pl);

        facade.createCountryIfNotExists(pl);

        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        // not calling lightblueDAO in dual write phase, because this is also a read method
        Mockito.verifyZeroInteractions(lightblueDAO);
    }

    @Test
    public void dualReadPhaseCreateWithReadTest() {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountryIfNotExists(pl)).thenReturn(createdByLegacy);

        Country createdCountry = facade.createCountryIfNotExists(pl);
        Assert.assertEquals(101l, createdCountry.getId());


        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == (Long)((DAOFacadeBase)facade).getEntityIdStore().pop());
    }

    @Test
    public void ligtblueProxyPhaseCreateWithReadTest() {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        facade.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
    }

    /* lightblue failure tests */

    @Test
    public void ligtblueFailureDuringReadTest() {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(pl);
        Mockito.when(lightblueDAO.getCountry(Mockito.anyString())).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = facade.getCountry("PL");

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void ligtblueFailureDuringUpdateTest() {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.updateCountry(pl)).thenReturn(ca);
        Mockito.when(lightblueDAO.updateCountry(Mockito.any(Country.class))).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = facade.updateCountry(pl);

        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(legacyDAO).updateCountry(pl);

        Assert.assertEquals(ca, returnedCountry);
    }

    @Test
    public void ligtblueFailureDuringCreateTest() {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(pl);
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = facade.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);

        Assert.assertEquals(pl, returnedCountry);
    }

}
