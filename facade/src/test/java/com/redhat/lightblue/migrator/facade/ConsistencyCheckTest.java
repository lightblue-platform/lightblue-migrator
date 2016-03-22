package com.redhat.lightblue.migrator.facade;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.model.CountryInCountry;
import com.redhat.lightblue.migrator.facade.model.CountryWithBigDecimal;
import com.redhat.lightblue.migrator.facade.model.CountryWithDate;
import com.redhat.lightblue.migrator.facade.model.ExtendedCountry;
import com.redhat.lightblue.migrator.facade.model.VeryExtendedCountry;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreSetter;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;

@RunWith(MockitoJUnitRunner.class)
public class ConsistencyCheckTest {

    @Mock
    private Logger inconsistencyLog;

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    CountryDAO legacyDAO = Mockito.mock(CountryDAO.class, Mockito.withSettings().extraInterfaces(SharedStoreSetter.class));
    CountryDAO lightblueDAO = Mockito.mock(CountryDAO.class, Mockito.withSettings().extraInterfaces(SharedStoreSetter.class));
    ConsistencyChecker consistencyChecker;

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        consistencyChecker = new ConsistencyChecker(CountryDAO.class.getSimpleName());
        consistencyChecker.setInconsistencyLog(inconsistencyLog);
    }

    @After
    public void after() {
        // no errors logged to inconsistency log
        Mockito.verify(inconsistencyLog, Mockito.never()).error(Mockito.anyString());
    }

    @Test
    public void testConsistencyWithIgnoredFiled() {
        Country pl1 = new Country(1l, "PL");
        pl1.setName("Poland1");

        Country pl2 = new Country(1l, "PL");
        pl2.setName("Poland2");

        Assert.assertTrue(consistencyChecker.checkConsistency(pl1, pl2));
    }

    @Test
    public void testConsistencyWithNull() {
        Assert.assertTrue(consistencyChecker.checkConsistency(null, null)); // no log
        Assert.assertFalse(consistencyChecker.checkConsistency(null, new Country()));
        Assert.assertFalse(consistencyChecker.checkConsistency(new Country(), null));
        Mockito.verify(inconsistencyLog, Mockito.times(2)).warn(Mockito.anyString());

        Country c1 = new Country(1l, null);
        Country c2 = new Country(1l, null);

        Assert.assertTrue(consistencyChecker.checkConsistency(c1, c2));

        Assert.assertTrue(consistencyChecker.checkConsistency(null, null));
        Assert.assertFalse(consistencyChecker.checkConsistency(c1, null));
        Assert.assertFalse(consistencyChecker.checkConsistency(null, c1));

        // 4 inconsistencies means 4 warnings
        Mockito.verify(inconsistencyLog, Mockito.times(4)).warn(Mockito.anyString());
    }

    @Test
    public void testInconsistency() {
        Country pl1 = new Country(1l, "PL");
        Country pl2 = new Country(2l, "PL");

        Assert.assertFalse(consistencyChecker.checkConsistency(pl1, pl2));
    }

    @Test
    public void testConsistencyList() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(consistencyChecker.checkConsistency(Arrays.asList(cArr1), Arrays.asList(cArr2)));
    }

    @Test
    public void testInconsistencyList() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(3l, "PL"), new Country(2l, "CA")};

        Assert.assertFalse(consistencyChecker.checkConsistency(Arrays.asList(cArr1), Arrays.asList(cArr2)));

        Country[] cArr3 = new Country[] { new Country(2l, "CA"), new Country(1l, "PL")};
        Country[] cArr4 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(consistencyChecker.checkConsistency(Arrays.asList(cArr3), Arrays.asList(cArr4)));
    }

    @Test
    public void testConsistencyArray() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(consistencyChecker.checkConsistency(cArr1, cArr2));
    }

    @Test
    public void testInconsistencyArray() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(3l, "PL"), new Country(2l, "CA")};

        Assert.assertFalse(consistencyChecker.checkConsistency(cArr1, cArr2));

        Country[] cArr3 = new Country[] { new Country(2l, "CA"), new Country(1l, "PL")};
        Country[] cArr4 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(consistencyChecker.checkConsistency(cArr3, cArr4));
    }

    @Test
    public void testConsistencyCheckAnnotationInheritence() {
        Country pl1 = new ExtendedCountry(1l, "PL");
        pl1.setName("Poland1");
        Country pl2 = new ExtendedCountry(1l, "PL");
        pl2.setName("Poland2");

        Assert.assertTrue(consistencyChecker.checkConsistency(pl1, pl2));
    }

    // NOT using Lenient
    /*@Test
    public void testTypeMismatch() {
        Country pl1 = new ExtendedCountry(1l, "PL");
        Country pl2 = new Country(1l, "PL");

        Assert.assertFalse(consistencyChecker.checkConsistency(pl1, pl2));

        // We are using compare mode = Lenient, which means object 2 can have additional data
        Assert.assertTrue(consistencyChecker.checkConsistency(pl2, pl1));
    }*/

    @Test
    public void testInaccessibleReqField_IsIgnored() {
        Country pl1 = new VeryExtendedCountry(1l, "PL", "foo");
        Country pl2 = new VeryExtendedCountry(1l, "PL", "bar");

        Assert.assertTrue(consistencyChecker.checkConsistency(pl1, pl2));
    }

    @Test
    public void testCountryInCountry_IsVerifiedForConsistency() {
        Country inner1 = new Country(2l, "CA");
        Country inner2 = new Country(2l, "CA2");

        Country pl1 = new CountryInCountry(1l, "PL", inner1);
        Country pl2 = new CountryInCountry(1l, "PL", inner2);

        Assert.assertFalse(consistencyChecker.checkConsistency(pl1, pl2));

        Assert.assertTrue(consistencyChecker.checkConsistency(pl1, new CountryInCountry(1l, "PL", new Country(2l, "CA"))));
    }

    @Test
    public void testSqlTimestamp() {
        Date date = new Date(1434638750978l);
        Timestamp timestamp = new Timestamp(date.getTime());

        CountryWithDate pl1 = new CountryWithDate(date);
        CountryWithDate pl2 = new CountryWithDate(timestamp);

        Assert.assertTrue(consistencyChecker.checkConsistency(pl1, pl2));
        Assert.assertTrue(consistencyChecker.checkConsistency(pl2, pl1));
    }

    @Test
    public void testSqlTimestampWithNanos() {
        Date date = new Date(1434638750978l);
        Timestamp timestamp = new Timestamp(date.getTime());
        timestamp.setNanos(timestamp.getNanos()+125300); // higher precision, difference in nano

        Assert.assertEquals(date.getTime(), timestamp.getTime());

        CountryWithDate pl1 = new CountryWithDate(date);
        CountryWithDate pl2 = new CountryWithDate(timestamp);

        Assert.assertTrue(consistencyChecker.checkConsistency(pl1, pl2));
        Assert.assertTrue(consistencyChecker.checkConsistency(pl2, pl1));
    }

    @Test
    public void testBigDecimalWithDifferentScale() {
        BigDecimal value1 = new BigDecimal(5000);
        BigDecimal value2 = new BigDecimal(5000.00);

        CountryWithBigDecimal pl1 = new CountryWithBigDecimal(value1);
        CountryWithBigDecimal pl2 = new CountryWithBigDecimal(value2);

        Assert.assertTrue(consistencyChecker.checkConsistency(pl1, pl2));
        Assert.assertTrue(consistencyChecker.checkConsistency(pl2, pl1));
    }

    // removed method inclusion implementation, it was slow
//    @Test
//    public void testWithMethodInclusion() {
//        Person p1 = new Person("John", "Doe", 35, "British");
//        Person p2 = new Person("John", "Doe", 35, "German");
//        Assert.assertTrue(consistencyChecker.checkConsistency(p1, p2, "getPerson", null));
//        Assert.assertFalse(consistencyChecker.checkConsistency(p1, p2, "getPerson2", null));
//
//        p1 = new Person("John", "Doe", 35, "British");
//        p2 = new Person("John", "Doe", 30, "British");
//        Assert.assertFalse(consistencyChecker.checkConsistency(p1, p2, "getPerson", null));
//        Assert.assertTrue(consistencyChecker.checkConsistency(p1, p2, "getPerson2", null));
//    }

    @Test
    public void testWithSimpleObjects() throws InterruptedException {
        Assert.assertTrue(consistencyChecker.checkConsistency("Test", "Test", "savePerson", null));
        Assert.assertFalse(consistencyChecker.checkConsistency("Test", "Test2", "savePerson", null));
        Assert.assertTrue(consistencyChecker.checkConsistency(true, true, "savePerson", null));
        Assert.assertFalse(consistencyChecker.checkConsistency(true, false, "savePerson", null));
        Assert.assertTrue(consistencyChecker.checkConsistency(100, 100, "savePerson", null));
        Assert.assertFalse(consistencyChecker.checkConsistency(100, 101, "savePerson", null));
        Date d = new Date();
        Assert.assertTrue(consistencyChecker.checkConsistency(d, d, "savePerson", null));
        Thread.sleep(10); // make sure dates below are inconsistent
        Assert.assertFalse(consistencyChecker.checkConsistency(d, new Date(), "savePerson", null));

    }
}
