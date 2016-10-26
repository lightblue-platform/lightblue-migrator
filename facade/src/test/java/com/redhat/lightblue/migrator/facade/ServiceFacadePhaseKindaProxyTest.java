package com.redhat.lightblue.migrator.facade;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;
import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

/**
 * Kinda proxy is like proxy phase but we're still writing to legacy and legacy is still
 * providing Lightblue with ids, hashes, timestampts, etc. It is possible to rollback from
 * kinda proxy, but it's not a lossless operation.
 *
 * @author mpatercz
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ServiceFacadePhaseKindaProxyTest extends ServiceFacadeTestBase {

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        super.setup();

        LightblueMigrationPhase.lightblueKindaProxyPhase(togglzRule);
    }

    @Test
    public void kindaProxyPhaseReadTest() throws CountryException {
        Mockito.when(lightblueDAO.getCountry("ca")).thenReturn(new Country("pl"));

        Country returnedCountry = countryDAOProxy.getCountry("ca");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO, Mockito.times(0)).getCountry("ca");
        Mockito.verify(lightblueDAO).getCountry("ca");

        // no consistency check, return data from Lightblue
        Assert.assertEquals("pl", returnedCountry.getIso2Code());
    }

    @Test
    public void kindaProxyPhaseReadTestWithNull() throws CountryException {
        Mockito.when(lightblueDAO.getCountry("ca")).thenReturn(null);

        Country returnedCountry = countryDAOProxy.getCountry("ca");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO, Mockito.times(0)).getCountry("ca");
        Mockito.verify(lightblueDAO).getCountry("ca");

        // no consistency check, return data from Lightblue
        Assert.assertEquals(null, returnedCountry);
    }

    @Test
    public void kindaProxyPhaseUpdateTest() throws CountryException {
        Country pl = new Country("PL");

        Mockito.when(lightblueDAO.updateCountry(pl)).thenReturn(new Country("pl"));

        Country returnedCountry = countryDAOProxy.updateCountry(pl);

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);

        // no consistency check, return data from Lightblue
        Assert.assertEquals("pl", returnedCountry.getIso2Code());
    }

    @Test
    public void kindaProxyPhaseCreateTest() throws CountryException {
        Country pl = new Country("PL");

        Mockito.when(lightblueDAO.createCountry(pl)).thenReturn(new Country("pl"));

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);

        // no consistency check, return data from Lightblue
        Assert.assertEquals("pl", returnedCountry.getIso2Code());
    }

    @Test
    public void kindaProxyPhaseCreateTimeoutDisabledTest() throws CountryException {
        Properties p = new Properties();
        TimeoutConfiguration t = new TimeoutConfiguration(-1, CountryDAO.class.getSimpleName(), p); // timeout disabled
        daoFacade.setTimeoutConfiguration(t);

        final Country pl = new Country("PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(new Country("ca"));
        Mockito.when(lightblueDAO.createCountry(pl)).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(300);
                return pl;
            }
        });

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);

        // no consistency check, no timeout, return data from Lightblue
        Assert.assertEquals("PL", returnedCountry.getIso2Code());
    }

    @Test
    public void kindaProxyPhaseCreateTimeoutIgnoredTest() throws CountryException {
        Properties p = new Properties();
        TimeoutConfiguration t = new TimeoutConfiguration(100, CountryDAO.class.getSimpleName(), p);
        daoFacade.setTimeoutConfiguration(t);

        final Country pl = new Country("PL");

        Mockito.when(legacyDAO.createCountry(pl)).thenReturn(new Country("ca"));
        Mockito.when(lightblueDAO.createCountry(pl)).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(300);
                return pl;
            }
        });

        Country returnedCountry = countryDAOProxy.createCountry(pl);

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(lightblueDAO).createCountry(pl);

        // no consistency check, no timeout, return data from Lightblue
        Assert.assertEquals("PL", returnedCountry.getIso2Code());
    }

    @Test
    public void ligtblueFailureDuringRead_KindaProxyPhase() throws CountryException {
        Mockito.doThrow(new CountryException()).when(lightblueDAO).getCountry("ca");

        try {
            countryDAOProxy.getCountry("ca");
            Assert.fail("Expected "+CountryException.class);
        } catch (CountryException e) {
            Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
            Mockito.verify(legacyDAO, Mockito.times(0)).getCountry("ca");
            Mockito.verify(lightblueDAO).getCountry("ca");
        }
    }

    @Test
    public void lightblueFailureDuringUpdate_KindaProxyPhase() throws CountryException {
        Country ca = new Country("ca");

        Mockito.when(legacyDAO.updateCountry(ca)).thenReturn(ca);
        Mockito.doThrow(new CountryException()).when(lightblueDAO).updateCountry(ca);

        try {
            countryDAOProxy.updateCountry(ca);
            Assert.fail("Expected "+CountryException.class);
        } catch (CountryException e) {
            Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
            Mockito.verify(legacyDAO).updateCountry(ca);
            Mockito.verify(lightblueDAO).updateCountry(ca);
        }
    }

    @Test
    public void lightblueFailureDuringCreate_KindaProxyPhase() throws CountryException {
        Country ca = new Country("ca");

        Mockito.when(legacyDAO.createCountry(ca)).thenReturn(ca);
        Mockito.doThrow(new CountryException()).when(lightblueDAO).createCountry(ca);

        try {
            countryDAOProxy.createCountry(ca);
            Assert.fail("Expected "+CountryException.class);
        } catch (CountryException e) {
            Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
            Mockito.verify(legacyDAO).createCountry(ca);
            Mockito.verify(lightblueDAO).createCountry(ca);
        }
    }

    @Test
    public void legacyFailureDuringRead_KindaProxyPhase() throws CountryException {
        Mockito.doThrow(new CountryException()).when(legacyDAO).getCountry("ca"); // it should not get called anyway...
        Mockito.when(lightblueDAO.getCountry("ca")).thenReturn(new Country("ca"));

        Country country = countryDAOProxy.getCountry("ca");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO, Mockito.times(0)).getCountry("ca");
        Mockito.verify(lightblueDAO).getCountry("ca");

        // consistency check disabled, legacy exception was swallowed and response from Lightblue returned
        Assert.assertEquals(new Country("ca"), country);
    }

    @Test
    public void legacyFailureDuringUpdate_KindaProxyPhase() throws CountryException {
        Country ca = new Country("ca");

        Mockito.doThrow(new CountryException()).when(legacyDAO).updateCountry(ca);
        Mockito.when(lightblueDAO.updateCountry(ca)).thenReturn(ca);

        Country country = countryDAOProxy.updateCountry(ca);

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO).updateCountry(ca);
        Mockito.verify(lightblueDAO).updateCountry(ca);

        // consistency check disabled, legacy exception was swallowed and response from Lightblue returned
        Assert.assertEquals(ca, country);
    }

    @Test
    public void legacyFailureDuringCreate_KindaProxyPhase() throws CountryException {
        Country ca = new Country("ca");

        Mockito.doThrow(new CountryException()).when(legacyDAO).createCountry(ca);
        Mockito.when(lightblueDAO.createCountry(ca)).thenReturn(ca);

        Country country = countryDAOProxy.createCountry(ca);

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verify(legacyDAO).createCountry(ca);
        Mockito.verify(lightblueDAO).createCountry(ca);

        // consistency check disabled, legacy exception was swallowed and response from Lightblue returned
        Assert.assertEquals(ca, country);
    }

}
