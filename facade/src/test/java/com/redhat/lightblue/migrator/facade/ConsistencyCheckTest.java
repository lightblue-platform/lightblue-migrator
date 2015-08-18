package com.redhat.lightblue.migrator.facade;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.facade.model.CountryWithDate;
import com.redhat.lightblue.migrator.facade.model.CountryInCountry;
import com.redhat.lightblue.migrator.facade.model.CountryWithBigDecimal;
import com.redhat.lightblue.migrator.facade.model.Person;
import com.redhat.lightblue.migrator.facade.model.VeryExtendedCountry;
import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.model.ExtendedCountry;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;

@RunWith(MockitoJUnitRunner.class)
public class ConsistencyCheckTest {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    @Mock CountryDAO legacyDAO;
    @Mock CountryDAOLightblue lightblueDAO;
    CountryDAO facade;
    DAOFacadeExample daoFacadeExample;

    @Before
    public void setup() {

        daoFacadeExample = new DAOFacadeExample(legacyDAO, lightblueDAO);
        facade = daoFacadeExample;
    }

    @Test
    public void testConsistencyWithIgnoredFiled() {
        Country pl1 = new Country(1l, "PL");
        pl1.setName("Poland1");

        Country pl2 = new Country(1l, "PL");
        pl2.setName("Poland2");

        Assert.assertTrue(daoFacadeExample.checkConsistency(pl1, pl2));
    }

    @Test
    public void testConsistencyWithNull() {
        Assert.assertTrue(daoFacadeExample.checkConsistency(null, null));
        Assert.assertFalse(daoFacadeExample.checkConsistency(null, new Country()));
        Assert.assertFalse(daoFacadeExample.checkConsistency(new Country(), null));

        Country c1 = new Country(1l, null);
        Country c2 = new Country(1l, null);

        Assert.assertTrue(daoFacadeExample.checkConsistency(c1, c2));
    }

    @Test
    public void testInconsistency() {
        Country pl1 = new Country(1l, "PL");
        Country pl2 = new Country(2l, "PL");

        Assert.assertFalse(daoFacadeExample.checkConsistency(pl1, pl2));
    }

    @Test
    public void testConsistencyList() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(daoFacadeExample.checkConsistency(Arrays.asList(cArr1), Arrays.asList(cArr2)));
    }

    @Test
    public void testInconsistencyList() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(3l, "PL"), new Country(2l, "CA")};

        Assert.assertFalse(daoFacadeExample.checkConsistency(Arrays.asList(cArr1), Arrays.asList(cArr2)));

        Country[] cArr3 = new Country[] { new Country(2l, "CA"), new Country(1l, "PL")};
        Country[] cArr4 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(daoFacadeExample.checkConsistency(Arrays.asList(cArr3), Arrays.asList(cArr4)));
    }

    @Test
    public void testConsistencyArray() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(daoFacadeExample.checkConsistency(cArr1, cArr2));
    }

    @Test
    public void testInconsistencyArray() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(3l, "PL"), new Country(2l, "CA")};

        Assert.assertFalse(daoFacadeExample.checkConsistency(cArr1, cArr2));

        Country[] cArr3 = new Country[] { new Country(2l, "CA"), new Country(1l, "PL")};
        Country[] cArr4 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(daoFacadeExample.checkConsistency(cArr3, cArr4));
    }

    @Test
    public void testConsistencyCheckAnnotationInheritence() {
        Country pl1 = new ExtendedCountry(1l, "PL");
        pl1.setName("Poland1");
        Country pl2 = new ExtendedCountry(1l, "PL");
        pl2.setName("Poland2");

        Assert.assertTrue(daoFacadeExample.checkConsistency(pl1, pl2));
    }

    @Test
    public void testTypeMismatch() {
        Country pl1 = new ExtendedCountry(1l, "PL");
        Country pl2 = new Country(1l, "PL");

        Assert.assertFalse(daoFacadeExample.checkConsistency(pl1, pl2));

        // We are using compare mode = Lenient, which means object 2 can have additional data
        Assert.assertTrue(daoFacadeExample.checkConsistency(pl2, pl1));
    }

    @Test
    public void testInaccessibleReqField_IsIgnored() {
        Country pl1 = new VeryExtendedCountry(1l, "PL", "foo");
        Country pl2 = new VeryExtendedCountry(1l, "PL", "bar");

        Assert.assertTrue(daoFacadeExample.checkConsistency(pl1, pl2));
    }

    @Test
    public void testCountryInCountry_IsVerifiedForConsistency() {
        Country inner1 = new Country(2l, "CA");
        Country inner2 = new Country(2l, "CA2");

        Country pl1 = new CountryInCountry(1l, "PL", inner1);
        Country pl2 = new CountryInCountry(1l, "PL", inner2);

        Assert.assertFalse(daoFacadeExample.checkConsistency(pl1, pl2));

        Assert.assertTrue(daoFacadeExample.checkConsistency(pl1, new CountryInCountry(1l, "PL", new Country(2l, "CA"))));
    }

    @Test
    public void testSqlTimestamp() {
        Date date = new Date(1434638750978l);
        Timestamp timestamp = new Timestamp(date.getTime());

        CountryWithDate pl1 = new CountryWithDate(date);
        CountryWithDate pl2 = new CountryWithDate(timestamp);

        Assert.assertTrue(daoFacadeExample.checkConsistency(pl1, pl2));
        Assert.assertTrue(daoFacadeExample.checkConsistency(pl2, pl1));
    }

    @Test
    public void testSqlTimestampWithNanos() {
        Date date = new Date(1434638750978l);
        Timestamp timestamp = new Timestamp(date.getTime());
        timestamp.setNanos(timestamp.getNanos()+125300); // higher precision, difference in nano

        Assert.assertEquals(date.getTime(), timestamp.getTime());

        CountryWithDate pl1 = new CountryWithDate(date);
        CountryWithDate pl2 = new CountryWithDate(timestamp);

        Assert.assertTrue(daoFacadeExample.checkConsistency(pl1, pl2));
        Assert.assertTrue(daoFacadeExample.checkConsistency(pl2, pl1));
    }

    @Test
    public void testBigDecimalWithDifferentScale() {
        BigDecimal value1 = new BigDecimal(5000);
        BigDecimal value2 = new BigDecimal(5000.00);

        CountryWithBigDecimal pl1 = new CountryWithBigDecimal(value1);
        CountryWithBigDecimal pl2 = new CountryWithBigDecimal(value2);

        Assert.assertTrue(daoFacadeExample.checkConsistency(pl1, pl2));
        Assert.assertTrue(daoFacadeExample.checkConsistency(pl2, pl1));
    }

    @Test
    public void testWithMethodInclusion() {
        Person p1 = new Person("John", "Doe", 35, "British");
        Person p2 = new Person("John", "Doe", 35, "German");
        Assert.assertTrue(daoFacadeExample.checkConsistency(p1, p2, "getPerson", null));
        Assert.assertFalse(daoFacadeExample.checkConsistency(p1, p2, "getPerson2", null));

        p1 = new Person("John", "Doe", 35, "British");
        p2 = new Person("John", "Doe", 30, "British");
        Assert.assertFalse(daoFacadeExample.checkConsistency(p1, p2, "getPerson", null));
        Assert.assertTrue(daoFacadeExample.checkConsistency(p1, p2, "getPerson2", null));
    }
}
