package com.redhat.lightblue.migrator.facade;

import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;
import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreSetter;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

@RunWith(MockitoJUnitRunner.class)
public class ServiceFacadeTest {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    CountryDAOFacadable legacyDAO = Mockito.mock(CountryDAOFacadable.class);
    CountryDAOFacadable lightblueDAO = Mockito.mock(CountryDAOFacadable.class);
    CountryDAO countryDAOProxy;

    @Spy ServiceFacade<CountryDAOFacadable> daoFacade = new ServiceFacade<CountryDAOFacadable>(legacyDAO, lightblueDAO, "CountryDAOFacade");
    @Spy ConsistencyChecker consistencyChecker = new ConsistencyChecker(CountryDAO.class.getSimpleName());

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        daoFacade.setConsistencyChecker(consistencyChecker);

        // countryDAOProxy is daoFacade using CountryDAO interface to invoke methods
        countryDAOProxy = FacadeProxyFactory.createFacadeProxy(daoFacade, CountryDAOFacadable.class);

        Mockito.verify(legacyDAO).setSharedStore((daoFacade).getSharedStore());
        Mockito.verify(lightblueDAO).setSharedStore((daoFacade).getSharedStore());
    }

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verifyNoMoreInteractions(legacyDAO);
    }

    @Test
    public void testGetCountryFromLegacy_Implicit() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        countryDAOProxy.getCountryFromLegacy(1l);

        Mockito.verify(legacyDAO).getCountryFromLegacy(1l);
        // even though this is a proxy phase, never call lightblue. It's not a facade operation.
        Mockito.verifyZeroInteractions(lightblueDAO);
    }

    @Test
    public void testGetCountryFromLegacy_Explicit() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        countryDAOProxy.getCountryFromLegacy2(1l);

        Mockito.verify(legacyDAO).getCountryFromLegacy2(1l);
        // even though this is a proxy phase, never call lightblue. It's not a facade operation.
        Mockito.verifyZeroInteractions(lightblueDAO);
    }

    @Test
    public void testGetCountryFromLightblue() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        countryDAOProxy.getCountryFromLightblue(1l);

        Mockito.verify(lightblueDAO).getCountryFromLightblue(1l);
        // even though this is an initial phase, never call legacy. It's not a facade operation.
        Mockito.verifyZeroInteractions(legacyDAO);
    }

    public interface CountryDAOSubinterface extends CountryDAO {
        public abstract Country getCountryFromLegacy(long id) throws CountryException;
    }

    /**
     * Calls to non annotated facade methods are proxied directly to legacy service. This test ensures this invocation
     * works when facade is created using an interface which extends the interface used by the service.
     *
     */
    @Test
    public void testGetCountryFromLegacy_UsingSubinterfaceToCreateProxy() throws Exception {
        // countryDAO is daoFacade using CountryDAO interface to invoke methods
        countryDAOProxy = FacadeProxyFactory.createFacadeProxy(daoFacade, CountryDAOSubinterface.class);

        Mockito.verify((SharedStoreSetter)legacyDAO).setSharedStore((daoFacade).getSharedStore());
        Mockito.verify((SharedStoreSetter)lightblueDAO).setSharedStore((daoFacade).getSharedStore());

        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        countryDAOProxy.getCountryFromLegacy(1l);

        Mockito.verify(legacyDAO).getCountryFromLegacy(1l);
        // even though this is a proxy phase, never call lightblue. It's not a facade operation.
        Mockito.verifyZeroInteractions(lightblueDAO);
    }



    /* Read tests */

    @Test
    public void initialPhaseRead() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        countryDAOProxy.getCountry("PL");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void dualReadPhaseReadConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country country = new Country();

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(country);
        Mockito.when(lightblueDAO.getCountry("PL")).thenReturn(country);

        countryDAOProxy.getCountry("PL");

        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
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

        Country returnedCountry = countryDAOProxy.getCountry("PL");

        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
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

        Country returnedCountry = countryDAOProxy.getCountries(ids).get(0);

        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO).getCountries(ids);
        Mockito.verify(lightblueDAO).getCountries(ids);

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, returnedCountry);
    }

    @Test
    public void lightblueProxyTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        countryDAOProxy.getCountry("PL");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).getCountry("PL");
    }

    /* update tests */

    @Test
    public void initialPhaseUpdate() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAOProxy.updateCountry(pl);

        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void dualWritePhaseUpdateConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");

        countryDAOProxy.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void dualWritePhaseUpdateInconsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country ca = new Country("CA");

        Mockito.when(legacyDAO.updateCountry(pl)).thenReturn(ca);
        Mockito.when(lightblueDAO.updateCountry(pl)).thenReturn(pl);

        Country updatedEntity = countryDAOProxy.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        // when there is a conflict, facade will return what legacy dao returned
        Assert.assertEquals(ca, updatedEntity);
    }

    @Test
    public void ligtblueProxyPhaseUpdateTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAOProxy.updateCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    /* insert tests */

    @Test
    public void initialPhaseCreate() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAOProxy.createCountry(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void dualWritePhaseCreateConsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);

        Country createdCountry = countryDAOProxy.createCountry(pl);
        Assert.assertEquals(new Long(101), createdCountry.getId());


        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void dualWritePhaseCreateInconsistentTest() throws CountryException {
        LightblueMigrationPhase.dualWritePhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(createdByLegacy);
        Mockito.when(lightblueDAO.createCountry(pl)).thenReturn(pl);

        Country createdCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl.getIso2Code(), "PL");
    }

    @Test
    public void ligtblueProxyPhaseCreateTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        // lightblue will handle ID generation in this phase
        daoFacade.setSharedStore(null);
        Mockito.verify((SharedStoreSetter)legacyDAO).setSharedStore(null);
        Mockito.verify((SharedStoreSetter)lightblueDAO).setSharedStore(null);

        Country pl = new Country("PL");

        countryDAOProxy.createCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    /* insert tests when method also does a read */

    @Test
    public void initialPhaseCreateWithRead() throws CountryException {
        LightblueMigrationPhase.initialPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAOProxy.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void dualReadPhaseCreateWithReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");
        Country createdByLegacy = new Country(101l, "PL"); // has id set

        Mockito.when(legacyDAO.createCountryIfNotExists(pl)).thenReturn(createdByLegacy);

        Country createdCountry = countryDAOProxy.createCountryIfNotExists(pl);
        Assert.assertEquals(new Long(101), createdCountry.getId());


        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void ligtblueProxyPhaseCreateWithReadTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Country pl = new Country("PL");

        countryDAOProxy.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    /* lightblue failure tests */

    @Test
    public void ligtblueFailureDuringReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Country pl = new Country("PL");

        Mockito.when(legacyDAO.getCountry("PL")).thenReturn(pl);
        Mockito.when(lightblueDAO.getCountry(Mockito.anyString())).thenThrow(new RuntimeException("Lightblue failure!"));

        Country returnedCountry = countryDAOProxy.getCountry("PL");

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

        Country returnedCountry = countryDAOProxy.updateCountry(pl);

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

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void ligtblueFailureDuringReadProxyPhaseTest() throws CountryException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        Mockito.doThrow(new CountryException()).when(lightblueDAO).getCountry("PL");

        try {
            countryDAOProxy.getCountry("PL");
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
            countryDAOProxy.createCountry(pl);
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
            countryDAOProxy.updateCountry(pl);
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

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);

        Assert.assertEquals(null, returnedCountry);
    }

    /* lightblue timeout tests */

    @Test
    public void lightblueTakesLongToRespondOnCreate_TimoutDisabled() throws CountryException {
        TimeoutConfiguration t = new TimeoutConfiguration(0, CountryDAO.class.getSimpleName(), null);
        daoFacade.setTimeoutConfiguration(t);

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

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_Success() throws CountryException {
        TimeoutConfiguration t = new TimeoutConfiguration(1000, CountryDAO.class.getSimpleName(), null);
        daoFacade.setTimeoutConfiguration(t);

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

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_Timeout() throws CountryException {
        TimeoutConfiguration t = new TimeoutConfiguration(1000, CountryDAO.class.getSimpleName(), null);
        daoFacade.setTimeoutConfiguration(t);

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

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnRead_Timeout() throws CountryException {
        TimeoutConfiguration t = new TimeoutConfiguration(1000, CountryDAO.class.getSimpleName(), null);
        daoFacade.setTimeoutConfiguration(t);

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

        Country returnedCountry = countryDAOProxy.getCountry("PL");

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnUpdate_Timeout() throws CountryException {
        TimeoutConfiguration t = new TimeoutConfiguration(1000, CountryDAO.class.getSimpleName(), null);
        daoFacade.setTimeoutConfiguration(t);

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

        Country returnedCountry = countryDAOProxy.updateCountry(pl);

        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnRead_Success_FromProperties_Method() throws CountryException {
        Properties p = new Properties();
        p.setProperty(TimeoutConfiguration.CONFIG_PREFIX+"timeout.CountryDAO", "1000");
        p.setProperty(TimeoutConfiguration.CONFIG_PREFIX+"timeout.CountryDAO.getCountry", "2000");
        TimeoutConfiguration t = new TimeoutConfiguration(500, CountryDAO.class.getSimpleName(), p);
        daoFacade.setTimeoutConfiguration(t);

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

        Country returnedCountry = countryDAOProxy.getCountry("PL");

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verify(legacyDAO).getCountry("PL");
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnRead_Timeout_FromProperties_Bean() throws CountryException {
        Properties p = new Properties();
        p.setProperty(TimeoutConfiguration.CONFIG_PREFIX+"timeout.CountryDAO", "1000");
        p.setProperty(TimeoutConfiguration.CONFIG_PREFIX+"timeout.CountryDAO.getCountry", "2000");
        TimeoutConfiguration t = new TimeoutConfiguration(500, CountryDAO.class.getSimpleName(), p);
        daoFacade.setTimeoutConfiguration(t);

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

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_ProxyPhase() throws CountryException {
        Properties p = new Properties();
        TimeoutConfiguration t = new TimeoutConfiguration(100, CountryDAO.class.getSimpleName(), p);
        daoFacade.setTimeoutConfiguration(t);

        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        final Country pl = new Country(101l, "PL");

        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(200);
                return pl;
            }
        });

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verifyZeroInteractions(consistencyChecker);

        Assert.assertEquals(pl, returnedCountry);
    }

    @Test
    public void lightblueTakesLongToRespondOnRead_ProxyPhase() throws CountryException {
        Properties p = new Properties();
        TimeoutConfiguration t = new TimeoutConfiguration(100, CountryDAO.class.getSimpleName(), p);
        daoFacade.setTimeoutConfiguration(t);

        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        final Country pl = new Country(101l, "PL");

        Mockito.when(lightblueDAO.getCountry(Mockito.any(String.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(200);
                return pl;
            }
        });

        Country returnedCountry = countryDAOProxy.getCountry("PL");

        Mockito.verify(lightblueDAO).getCountry("PL");
        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verifyZeroInteractions(consistencyChecker);

        Assert.assertEquals(pl, returnedCountry);
    }

    /* legacy failure tests */

    @Test
    public void legacyFailureDuringParallelReadTest() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        Mockito.when(legacyDAO.getCountry("PL")).then(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(100);
                throw new CountryException();
            }
        });

        try {
            countryDAOProxy.getCountry("PL");
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

        final Country pl = new Country("PL");

        Mockito.when(legacyDAO.updateCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(100);
                throw new CountryException();
            }

        });

        try {
            countryDAOProxy.updateCountry(pl);
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
            countryDAOProxy.createCountry(pl);
            Assert.fail();
        } catch(CountryException ce) {

        } catch(Exception e) {
            Assert.fail();
        }

        Mockito.verify(legacyDAO).createCountry(pl);
    }

    /**
     * This test ensures that shared data is cleared for current thread at the beginning of execution.
     *
     * Thread Ids are unique, but can be reused. That means shared data created by previous thread
     * can be accessed by the current thread with the same id, assuming the data does not expire and it is
     * not consumed by the originating thread (there is an error before shared data is consumed).
     *
     * @throws CountryException
     */
    @Test
    public void clearSharedStoreAfterFailure() throws CountryException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        final Country pl = new Country("PL");
        final Country ca = new Country("CA");

        // legacyDAO creates CA Country with id=12
        // legacyDAO creates PL Country with id=13
        Mockito.when(legacyDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Country country = (Country)invocation.getArguments()[0];
                if (country.getIso2Code().equals("CA")){
                    // oracle service pushes id of a created object into shared store
                    daoFacade.getSharedStore().push(12l);
                    return new Country(12l, "CA");
                } else {
                    // oracle service pushes id of a created object into shared store
                    daoFacade.getSharedStore().push(13l);
                    return new Country(13l, "PL");
                }
            }

        });

        // lightblueDAO fails when creating CA Country
        // lightblueDAO creates PL Country with id=13
        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Country country = (Country)invocation.getArguments()[0];
                if (country.getIso2Code().equals("CA")){
                    throw new CountryException();
                } else {
                    return new Country((Long)daoFacade.getSharedStore().pop(), "PL");
                }
            }

        });

        // lightblue service operation fails and the id is not retrieved
        countryDAOProxy.createCountry(ca);

        // lightblue service operation succeeds, id is retrieved from shared store
        // reusing same thread id as the previous call
        Country returned = countryDAOProxy.createCountry(pl);

        Assert.assertEquals((Long)13l, returned.getId());

        // the id pushed into shared store during first, failed call, is cleared, expecting no inconsistency
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.eq(new Country(13l, "PL")), Mockito.eq(new Country(13l, "PL")), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));

        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(legacyDAO).createCountry(ca);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(ca);
    }
}
