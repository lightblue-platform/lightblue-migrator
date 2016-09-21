package com.redhat.lightblue.migrator.facade;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;
import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

/**
 * Dual write phase tests. Dual write means that both legacy and Lightblue are being written to, but only
 * legacy is being read.
 *
 * @author mpatercz
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ServiceFacadePhaseDualWriteTest extends ServiceFacadeTestBase {

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        super.setup();

        LightblueMigrationPhase.dualWritePhase(togglzRule);
    }

    @Test
    public void dualWritePhaseUpdateConsistentTest() throws CountryException {
        Country pl = new Country("PL");

        countryDAOProxy.updateCountry(pl);

        Mockito.verify(legacyDAO).updateCountry(pl);
        Mockito.verify(lightblueDAO).updateCountry(pl);
        Mockito.verify(consistencyChecker).checkConsistency(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(MethodCallStringifier.class));
    }

    @Test
    public void dualWritePhaseUpdateInconsistentTest() throws CountryException {
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
    public void dualWritePhaseCreateConsistentTest() throws CountryException {
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


}
