package com.redhat.lightblue.migrator.consistency;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class BeanConsistencyCheckerTest {

    BeanConsistencyChecker beanConsistencyChecker = new BeanConsistencyChecker();

    @Test
    public void testConsistencyWithIgnoredFiled() {
        Country pl1 = new Country(1l, "PL");
        pl1.setName("Poland1");

        Country pl2 = new Country(1l, "PL");
        pl2.setName("Poland2");

        Assert.assertTrue(beanConsistencyChecker.consistent(pl1, pl2));
    }

    @Test
    public void testConsistencyWithNull() {
        Assert.assertTrue(beanConsistencyChecker.consistent(null, null));
        Assert.assertFalse(beanConsistencyChecker.consistent(null, new Country()));
        Assert.assertFalse(beanConsistencyChecker.consistent(new Country(), null));

        Country c1 = new Country(1l, null);
        Country c2 = new Country(1l, null);

        Assert.assertTrue(beanConsistencyChecker.consistent(c1, c2));
    }

    @Test
    public void testInconsistency() {
        Country pl1 = new Country(1l, "PL");
        Country pl2 = new Country(2l, "PL");

        Assert.assertFalse(beanConsistencyChecker.consistent(pl1, pl2));
    }

    @Test
    public void testConsistencyList() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertTrue(beanConsistencyChecker.consistent(Arrays.asList(cArr1), Arrays.asList(cArr2)));
    }

    @Test
    public void testInconsistencyList() {
        Country[] cArr1 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};
        Country[] cArr2 = new Country[] { new Country(3l, "PL"), new Country(2l, "CA")};

        Assert.assertFalse(beanConsistencyChecker.consistent(Arrays.asList(cArr1), Arrays.asList(cArr2)));

        Country[] cArr3 = new Country[] { new Country(2l, "CA"), new Country(1l, "PL")};
        Country[] cArr4 = new Country[] { new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertFalse(beanConsistencyChecker.consistent(Arrays.asList(cArr3), Arrays.asList(cArr4)));
    }

    @Test
    public void testConsistencyCheckAnnotationInheritence() {
        Country pl1 = new ExtendedCountry(1l, "PL");
        pl1.setName("Poland1");
        Country pl2 = new ExtendedCountry(1l, "PL");
        pl2.setName("Poland2");

        Assert.assertTrue(beanConsistencyChecker.consistent(pl1, pl2));
    }

    @Test
    public void testTypeMismatch() {
        Country pl1 = new ExtendedCountry(1l, "PL");
        Country pl2 = new Country(1l, "PL");

        Assert.assertFalse(beanConsistencyChecker.consistent(pl1, pl2));
        Assert.assertFalse(beanConsistencyChecker.consistent(pl2, pl1));
    }

    @Test
    public void testInaccessibleReqField_IsIgnored() {
        Country pl1 = new VeryExtendedCountry(1l, "PL", "foo");
        Country pl2 = new VeryExtendedCountry(1l, "PL", "bar");

        Assert.assertTrue(beanConsistencyChecker.consistent(pl1, pl2));
    }

    @Test
    public void testCountryInCountry_IsVerifiedForConsistency() {
        Country inner1 = new Country(2l, "CA");
        Country inner2 = new Country(2l, "CA2");

        Country pl1 = new CountryInCountry(1l, "PL", inner1);
        Country pl2 = new CountryInCountry(1l, "PL", inner2);

        Assert.assertFalse(beanConsistencyChecker.consistent(pl1, pl2));

        Assert.assertTrue(beanConsistencyChecker.consistent(pl1, new CountryInCountry(1l, "PL", new Country(2l, "CA"))));
    }

    @Test
    public void testSqlTimestamp() {
        Date date = new Date(1434638750978l);
        Timestamp timestamp = new Timestamp(date.getTime());

        CountryWithDate pl1 = new CountryWithDate(date);
        CountryWithDate pl2 = new CountryWithDate(timestamp);

        Assert.assertTrue(beanConsistencyChecker.consistent(pl1, pl2));
        Assert.assertTrue(beanConsistencyChecker.consistent(pl2, pl1));
    }

    @Test
    public void testSqlTimestampWithNanos() {
        Date date = new Date(1434638750978l);
        Timestamp timestamp = new Timestamp(date.getTime());
        timestamp.setNanos(timestamp.getNanos()+125300); // higher precision, difference in nano

        Assert.assertEquals(date.getTime(), timestamp.getTime());

        CountryWithDate pl1 = new CountryWithDate(date);
        CountryWithDate pl2 = new CountryWithDate(timestamp);

        Assert.assertTrue(beanConsistencyChecker.consistent(pl1, pl2));
        Assert.assertTrue(beanConsistencyChecker.consistent(pl2, pl1));
    }

}
