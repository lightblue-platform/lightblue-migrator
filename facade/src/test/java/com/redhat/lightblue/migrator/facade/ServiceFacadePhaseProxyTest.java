package com.redhat.lightblue.migrator.facade;

import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;
import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreSetter;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

/**
 * Full proxy tests. Full proxy means only Lightblue is used for both reads and writes. There is no fallback.
 *
 * @author mpatercz
 *
 */
public class ServiceFacadePhaseProxyTest extends ServiceFacadeTestBase {

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        super.setup();

        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);
    }

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verifyNoMoreInteractions(legacyDAO);
    }

    @Test(expected=IllegalStateException.class)
    public void testGetCountryFromLegacy_ProxyPhase_Implicit() throws CountryException {
        // throws exception because this method is facade and legacy/source is no more
        countryDAOProxy.getCountryFromLegacy(1l);
    }

    @Test
    public void testGetCountryFromLegacy_Explicit() throws CountryException {
        countryDAOProxy.getCountryFromLegacy2(1l);

        Mockito.verify(legacyDAO).getCountryFromLegacy2(1l);
        // even though this is a proxy phase, never call lightblue,
        // since this operation is explicitly annotated as source
        Mockito.verifyZeroInteractions(lightblueDAO);
    }

    @Test
    public void lightblueProxyTest() throws CountryException {
        countryDAOProxy.getCountry("PL");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).getCountry("PL");
    }

    @Test
    public void ligtblueProxyPhaseUpdateTest() throws CountryException {
        Country pl = new Country("PL");

        countryDAOProxy.updateCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void ligtblueProxyPhaseCreateTest() throws CountryException {
        // lightblue will handle ID generation in this phase
        daoFacade.setSharedStore(null);
        Mockito.verify((SharedStoreSetter) legacyDAO).setSharedStore(null);
        Mockito.verify((SharedStoreSetter) lightblueDAO).setSharedStore(null);

        Country pl = new Country("PL");

        countryDAOProxy.createCountry(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void ligtblueProxyPhaseCreateWithReadTest() throws CountryException {
        Country pl = new Country("PL");

        countryDAOProxy.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(legacyDAO);
        Mockito.verify(lightblueDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void ligtblueFailureDuringReadProxyPhaseTest() throws CountryException {
        Mockito.doThrow(new CountryException()).when(lightblueDAO).getCountry("PL");

        try {
            countryDAOProxy.getCountry("PL");
            Assert.fail();
        } catch (CountryException ce) {

        } catch (Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).getCountry("PL");

    }

    @Test
    public void ligtblueFailureDuringCreateProxyPhaseTest() throws CountryException {
        Country pl = new Country(101l, "PL");

        Mockito.doThrow(new CountryException()).when(lightblueDAO).createCountry(pl);

        try {
            countryDAOProxy.createCountry(pl);
            Assert.fail();
        } catch (CountryException ce) {

        } catch (Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).createCountry(pl);

    }

    @Test
    public void ligtblueFailureDuringUpdateProxyPhaseTest() throws CountryException {
        Country pl = new Country(101l, "PL");

        Mockito.doThrow(new CountryException()).when(lightblueDAO).updateCountry(pl);

        try {
            countryDAOProxy.updateCountry(pl);
            Assert.fail();
        } catch (CountryException ce) {

        } catch (Exception e) {
            Assert.fail();
        }

        Mockito.verify(lightblueDAO).updateCountry(pl);

    }

    @Test
    public void lightblueTakesLongToRespondOnCreate_ProxyPhase() throws CountryException {
        Properties p = new Properties();
        TimeoutConfiguration t = new TimeoutConfiguration(100, CountryDAO.class.getSimpleName(), p);
        daoFacade.setTimeoutConfiguration(t);

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

}
