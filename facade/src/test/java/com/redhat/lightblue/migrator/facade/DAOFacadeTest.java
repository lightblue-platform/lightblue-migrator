package com.redhat.lightblue.migrator.facade;

import java.util.Arrays;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

@RunWith(MockitoJUnitRunner.class)
public class DAOFacadeTest {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    @Mock CountryDAO legacyDAO;
    @Mock CountryDAOLightblue lightblueDAO;
    CountryDAO countryDAO;
    DAOFacadeBase<CountryDAO> daoFacade;

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        daoFacade =  Mockito.spy(new DAOFacadeBase<CountryDAO>(legacyDAO, (CountryDAO)lightblueDAO));

        // countryDAO is daoFacade using CountryDAO interface to invoke methods
        countryDAO = FacadeProxyFactory.createFacadeProxy(daoFacade, CountryDAO.class);
        Mockito.verify(lightblueDAO).setEntityIdStore((daoFacade).getEntityIdStore());
    }

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verifyNoMoreInteractions(legacyDAO);
    }

    @Test
    public void testGetCountryFromLegacy() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        countryDAO.getCountryFromLegacy(1l);

        Mockito.verify(legacyDAO).getCountryFromLegacy(1l);
        // even though this is a proxy phase, never call lightblue. It's not a facade operation.
        Mockito.verifyZeroInteractions(lightblueDAO);
    }

    /* Read tests */

    @Test
    public void initialPhaseRead() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        countryDAO.getCountry("PL");

        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void dualReadPhaseReadConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country country = new Country();

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(country);
        Mockito.when(lightblueDAO.getCountry("PL")).thenReturn(country);

        countryDAO.getCountry("PL");

        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(lightblueDAO).getCountry("PL");
    }

    @Test
    public void dualReadPhaseReadInconsistentTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(ca);
        Mockito.when(lightblueDAO.getCountry("PL")).thenReturn(pl);

        Country returnedCountry = countryDAO.getCountry("PL");

        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(lightblueDAO).getCountry("PL");

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, returnedCountry);
    }

    @Test
    public void dualReadPhaseReadInconsistentPrimitiveArrayTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country(1l, "PL");
        Country ca = new Country(2l, "CA");

        long[] ids = new long[]{1l,2l,3l};

        Mockito.when(legacyDAO.getCountries(ids)).thenReturn(Arrays.asList(new Country[] {ca}));
        Mockito.when(lightblueDAO.getCountries(ids)).thenReturn(Arrays.asList(new Country[] {pl}));

        Country returnedCountry = countryDAO.getCountries(ids).get(0);

        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(legacyDAO).getCountries(ids);
        Mockito.verify(lightblueDAO).getCountries(ids);

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, returnedCountry);
    }

    @Test
    public void lightblueProxyTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        countryDAO.getCountry("PL");

        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).getCountry("PL");
    }

    /* update tests */

    @Test
    public void initialPhaseUpdate() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAO.updateCountry(pl);

        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualWritePhaseUpdateConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");

        countryDAO.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualWritePhaseUpdateInconsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.updateCountry(pl)).thenReturn(ca);
        Mockito.when(lightblueDAO.updateCountry(pl)).thenReturn(pl);

        Country updatedEntity = countryDAO.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, updatedEntity);
    }

    @Test
    public void ligtblueProxyPhaseUpdateTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAO.updateCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    /* insert tests */

    @Test
    public void initialPhaseCreate() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAO.createCountry(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualWritePhaseCreateConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);

        Country createdCountry = countryDAO.createCountry(pl);
        Assert.assertEquals(new Long(101), createdCountry.getId());


        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == (Long)daoFacade.getEntityIdStore().pop());
    }

    @Test
    public void dualWritePhaseCreateInconsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);
        Mockito.when(lightblueDAO.createCountry(pl)).thenReturn(pl);

        Country createdCountry = countryDAO.createCountry(pl);

        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == (Long) daoFacade.getEntityIdStore().pop());

        Assert.assertEquals(pl.getIso2Code(), "PL");
    }

    @Test
    public void ligtblueProxyPhaseCreateTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        // lightblue will handle ID generation in this phase
        daoFacade.setEntityIdStore(null);
        Mockito.verify(lightblueDAO).setEntityIdStore(null);

        Country pl = new Country("PL");

        countryDAO.createCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    /* insert tests when method also does a read */

    @Test
    public void initialPhaseCreateWithRead() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAO.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualReadPhaseCreateWithReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountryIfNotExists(pl)).thenReturn(createdByLegacy);

        Country createdCountry = countryDAO.createCountryIfNotExists(pl);
        Assert.assertEquals(new Long(101), createdCountry.getId());


        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == (Long)daoFacade.getEntityIdStore().pop());
    }

    @Test
    public void ligtblueProxyPhaseCreateWithReadTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAO.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    /* lightblue failure tests */

    @Test
    public void ligtblueFailureDuringReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(pl);
        Mockito.when(lightblueDAO.getCountry(Mockito.anyString())).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = countryDAO.getCountry("PL");

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueFailureDuringUpdateTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.updateCountry(pl)).thenReturn(ca);
        Mockito.when(lightblueDAO.updateCountry(Mockito.any(Country.class))).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = countryDAO.updateCountry(pl);

        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(legacyDAO).updateCountry(pl);

        Assert.assertEquals(ca, returnedCountry);
    }

    @Test
    public void lightblueFailureDuringCreateTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(pl);
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = countryDAO.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void ligtblueFailureDuringReadProxyPhaseTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Mockito.doThrow(new CountryException()).when(lightblueDAO).getCountry("PL");

        try {
            countryDAO.getCountry("PL");
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).getCountry("PL");

    }

    @Test
    public void ligtblueFailureDuringCreateProxyPhaseTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country(101l, "PL");

        Mockito.doThrow(new CountryException()).when(lightblueDAO).createCountry(pl);

        try {
            countryDAO.createCountry(pl);
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).createCountry(pl);

    }

    @Test
    public void ligtblueFailureDuringUpdateProxyPhaseTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country(101l, "PL");

        Mockito.doThrow(new CountryException()).when(lightblueDAO).updateCountry(pl);

        try {
            countryDAO.updateCountry(pl);
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).updateCountry(pl);

    }

    @Test
    public void lightblueNullReturnedAfterCreateTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(null);
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenReturn(null);

        Country returnedCountry = countryDAO.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);

        Assert.assertEquals(null, returnedCountry);
    }

    /* lightblue timeout tests */

    @Test
    public void lightblueTakesLongToRespondOnCreate_TimoutDisabled() throws CountryException {
        daoFacade.setTimeoutSeconds(0);
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        final Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(pl);
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(500);
                return pl;
            }

        });

        Country returnedCountry = countryDAO.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_Success() throws CountryException {
        daoFacade.setTimeoutSeconds(1);
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        final Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(pl);
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(500);
                return pl;
            }

        });

        Country returnedCountry = countryDAO.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(daoFacade).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_Timeout() throws CountryException {
        daoFacade.setTimeoutSeconds(1);
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        final Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(pl);
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(1500);
                return pl;
            }

        });

        Country returnedCountry = countryDAO.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnRead_Timeout() throws CountryException {
        daoFacade.setTimeoutSeconds(1);
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        final Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(pl);
        Mockito.when(lightblueDAO.getCountry("PL")).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(1500);
                return pl;
            }

        });

        Country returnedCountry = countryDAO.getCountry("PL");

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnUpdate_Timeout() throws CountryException {
        daoFacade.setTimeoutSeconds(1);
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        final Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.updateCountry(pl)).thenReturn(pl);
        Mockito.when(lightblueDAO.updateCountry(pl)).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(1500);
                return pl;
            }

        });

        Country returnedCountry = countryDAO.updateCountry(pl);

        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(daoFacade, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    /* legacy failure tests */

    @Test
    public void legacyFailureDuringReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Mockito.doThrow(new CountryException()).when(legacyDAO).getCountry("PL");

        try {
            countryDAO.getCountry("PL");
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void legacyFailureDuringUpdateTest() throws CountryException, InterruptedException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");

        Mockito.doThrow(new CountryException()).when(legacyDAO).updateCountry(pl);

        try {
            countryDAO.updateCountry(pl);
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        // deal with the race condition causing verifyNoMoreInteractions() to fail occasionally
        Thread.sleep(500);

        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(legacyDAO).updateCountry(pl);
    }

    @Test
    public void legacyFailureDuringCreateTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");

        Mockito.doThrow(new CountryException()).when(legacyDAO).createCountry(pl);

        try {
            countryDAO.createCountry(pl);
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(legacyDAO).createCountry(pl);
    }
}
