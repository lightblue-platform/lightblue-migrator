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
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

@RunWith(MockitoJUnitRunner.class)
@Deprecated
public class DAOFacadeTest {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    @Mock CountryDAO legacyDAO;
    @Mock CountryDAOLightblue lightblueDAO;
    CountryDAO facade;

    DAOFacadeExample daoFacadeExample;
    ConsistencyChecker consistencyChecker;

    @Before
    public void setup() {
        daoFacadeExample = Mockito.spy(new DAOFacadeExample(legacyDAO, lightblueDAO));
        consistencyChecker = Mockito.spy(new ConsistencyChecker(CountryDAO.class.getSimpleName()));
        daoFacadeExample.setConsistencyChecker(consistencyChecker);

        facade = daoFacadeExample;
        Mockito.verify(lightblueDAO).setEntityIdStore((daoFacadeExample).getEntityIdStore());
    }

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verifyNoMoreInteractions(legacyDAO);
    }

    /* Read tests */

    @Test
    public void initialPhaseRead() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        facade.getCountry("PL");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void dualReadPhaseReadConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country country = new Country();

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(country);
        Mockito.when(lightblueDAO.getCountry("PL")).thenReturn(country);

        facade.getCountry("PL");

        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
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

        Country returnedCountry = facade.getCountry("PL");

        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
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

        Country returnedCountry = facade.getCountries(ids).get(0);

        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(legacyDAO).getCountries(ids);
        Mockito.verify(lightblueDAO).getCountries(ids);

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, returnedCountry);
    }

    @Test
    public void lightblueProxyTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        facade.getCountry("PL");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).getCountry("PL");
    }

    /* update tests */

    @Test
    public void initialPhaseUpdate() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        facade.updateCountry(pl);

        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualWritePhaseUpdateConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");

        facade.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualWritePhaseUpdateInconsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.updateCountry(pl)).thenReturn(ca);
        Mockito.when(lightblueDAO.updateCountry(pl)).thenReturn(pl);

        Country updatedEntity = facade.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, updatedEntity);
    }

    @Test
    public void ligtblueProxyPhaseUpdateTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        facade.updateCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    /* insert tests */

    @Test
    public void initialPhaseCreate() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        facade.createCountry(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualWritePhaseCreateConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);

        Country createdCountry = facade.createCountry(pl);
        Assert.assertEquals(new Long(101), createdCountry.getId());


        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == ((DAOFacadeBase)facade).getEntityIdStore().pop());
    }

    @Test
    public void dualWritePhaseCreateInconsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);
        Mockito.when(lightblueDAO.createCountry(pl)).thenReturn(pl);

        Country createdCountry = facade.createCountry(pl);

        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == ((DAOFacadeBase) facade).getEntityIdStore().pop());

        Assert.assertEquals(pl.getIso2Code(), "PL");
    }

    @Test
    public void ligtblueProxyPhaseCreateTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        // lightblue will handle ID generation in this phase
        ((DAOFacadeBase) facade).setEntityIdStore(null);
        Mockito.verify(lightblueDAO).setEntityIdStore(null);

        Country pl = new Country("PL");

        facade.createCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    /* insert tests when method also does a read */

    @Test
    public void initialPhaseCreateWithRead() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        facade.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void dualReadPhaseCreateWithReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountryIfNotExists(pl)).thenReturn(createdByLegacy);

        Country createdCountry = facade.createCountryIfNotExists(pl);
        Assert.assertEquals(new Long(101), createdCountry.getId());


        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        // CountryDAOLightblue should set the id. Since it's just a mock, I'm checking what's in the cache.
        Assert.assertTrue(101l == ((DAOFacadeBase)facade).getEntityIdStore().pop());
    }

    @Test
    public void ligtblueProxyPhaseCreateWithReadTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        facade.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    /* lightblue failure tests */

    @Test
    public void ligtblueFailureDuringReadTest() throws CountryException {
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
    public void lightblueFailureDuringUpdateTest() throws CountryException {
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
    public void lightblueFailureDuringCreateTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(pl);
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = facade.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void ligtblueFailureDuringReadProxyPhaseTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Mockito.doThrow(new CountryException()).when(lightblueDAO).getCountry("PL");

        try {
            facade.getCountry("PL");
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
            facade.createCountry(pl);
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
            facade.updateCountry(pl);
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

        Country returnedCountry = facade.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);

        Assert.assertEquals(null, returnedCountry);
    }

    /* lightblue timeout tests */

    @Test
    public void lightblueTakesLongToRespondOnCreate_TimoutDisabled() throws CountryException {
        daoFacadeExample.setTimeoutSeconds(0);
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

        Country returnedCountry = facade.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_Success() throws CountryException {
        daoFacadeExample.setTimeoutSeconds(1);
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

        Country returnedCountry = facade.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_Timeout() throws CountryException {
        daoFacadeExample.setTimeoutSeconds(1);
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

        Country returnedCountry = facade.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnRead_Timeout() throws CountryException {
        daoFacadeExample.setTimeoutSeconds(1);
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

        Country returnedCountry = facade.getCountry("PL");

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnUpdate_Timeout() throws CountryException {
        daoFacadeExample.setTimeoutSeconds(1);
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

        Country returnedCountry = facade.updateCountry(pl);

        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        Assert.assertEquals(pl, returnedCountry);
    }

    /* legacy failure tests */

    @Test
    public void legacyFailureDuringReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Mockito.when(legacyDAO.getCountry(Mockito.any(String.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(100);
                throw new CountryException();
            }

        });

        try {
            facade.getCountry("PL");
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void legacyFailureDuringUpdateTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");

        Mockito.when(legacyDAO.updateCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(100);
                throw new CountryException();
            }

        });

        try {
            facade.updateCountry(pl);
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(legacyDAO).updateCountry(pl);
    }

    @Test
    public void legacyFailureDuringCreateTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");

        Mockito.when(legacyDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(100);
                throw new CountryException();
            }

        });

        try {
            facade.createCountry(pl);
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(legacyDAO).createCountry(pl);
    }

}
