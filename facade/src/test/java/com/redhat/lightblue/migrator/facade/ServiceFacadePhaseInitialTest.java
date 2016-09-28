package com.redhat.lightblue.migrator.facade;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;
import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

/**
 * Initial phase tests. Initial phase means only source system is used, Lightblue isn't.
 *
 * @author mpatercz
 *
 */
public class ServiceFacadePhaseInitialTest extends ServiceFacadeTestBase {

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        super.setup();

        LightblueMigrationPhase.initialPhase(togglzRule);
    }

    @Test
    public void testGetCountryFromLightblue() throws CountryException {
        countryDAOProxy.getCountryFromLightblue(1l);

        Mockito.verify(lightblueDAO).getCountryFromLightblue(1l);
        // even though this is an initial phase, never call legacy. It's not a facade operation.
        Mockito.verifyZeroInteractions(legacyDAO);
    }

    @Test
    public void initialPhaseRead() throws CountryException {
        countryDAOProxy.getCountry("PL");

        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).getCountry("PL");
    }

    @Test
    public void initialPhaseUpdate() throws CountryException {
        Country pl = new Country("PL");

        countryDAOProxy.updateCountry(pl);

        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void initialPhaseCreate() throws CountryException {
        Country pl = new Country("PL");

        countryDAOProxy.createCountry(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountry(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void initialPhaseCreateWithRead() throws CountryException {
        Country pl = new Country("PL");

        countryDAOProxy.createCountryIfNotExists(pl);

        Mockito.verifyZeroInteractions(lightblueDAO);
        Mockito.verify(legacyDAO).createCountryIfNotExists(pl);
        Mockito.verify(consistencyChecker, Mockito.never()).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

}
