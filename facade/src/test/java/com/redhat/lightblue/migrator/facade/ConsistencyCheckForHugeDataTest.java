package com.redhat.lightblue.migrator.facade;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.migrator.facade.model.Country;

/**
 * Tests the logic which uses JSONCompare only for non-huge responses.
 *
 * @author mpatercz
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsistencyCheckForHugeDataTest {

    private Logger inconsistencyLog = Mockito.spy(LoggerFactory.getLogger(ConsistencyChecker.class));

    ConsistencyChecker consistencyChecker;

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        consistencyChecker = new ConsistencyChecker(CountryDAO.class.getSimpleName());
        consistencyChecker.setInconsistencyLog(inconsistencyLog);
        consistencyChecker.setMaxJsonStrLengthForJsonCompare(1000);
    }

    @After
    public void after() {
        // no errors logged to inconsistency log
        Mockito.verify(inconsistencyLog, Mockito.never()).error(Mockito.anyString());
    }

    @Test
    public void testSmallDataInconsistencyLogsUsingJsonCompare() {
        Country pl1 = new Country(1l, "PL");

        Country pl2 = new Country(2l, "PL");

        Assert.assertFalse(consistencyChecker.checkConsistency(pl1, pl2));
        Mockito.verify(inconsistencyLog, Mockito.times(1)).warn(
                "[main] Inconsistency found in CountryDAO. - diff: id,Expected: 1,     got: 2, - legacyJson: {\"iso2Code\":\"PL\",\"iso3Code\":null,\"id\":1,\"neighbour\":null,\"allies\":null}, lightblueJson: {\"iso2Code\":\"PL\",\"iso3Code\":null,\"id\":2,\"neighbour\":null,\"allies\":null}"
                );
    }

    @Test
    public void testBigDataInconsistencyLogsUsingJiff() {

        List<Country> countries1 = new ArrayList<>();

        for (long i = 0; i < 100; i++) {
            countries1.add(new Country(i, "PL" + i));
        }

        List<Country> countries2 = new ArrayList<>(countries1);
        countries2.add(new Country(-1l, "PL"));
        countries1.set(0, new Country(-1l, "PL"));

        Assert.assertFalse(consistencyChecker.checkConsistency(countries1, countries2));
        ArgumentCaptor<String> loggedWarn = ArgumentCaptor.forClass(String.class);
        Mockito.verify(inconsistencyLog, Mockito.times(1)).warn(loggedWarn.capture());
        loggedWarn.getAllValues().get(0).startsWith("[main] Inconsistency found in CountryDAO. - diff: 100(null != {\"iso2Code\":\"PL\",\"iso3Code\":null,\"id\":-1,\"neighbour\":null})");
    }

    @Test
    public void testCompareJiffAndJsonCompareArrayElementInconsistencies() {
        List<Country> countries1 = new ArrayList<>();

        for (long i = 0; i < 100; i++) {
            countries1.add(new Country(i, "PL" + i));
        }

        List<Country> countries2 = new ArrayList<>(countries1);

        // create allied country inconsistency
        countries1.get(0).setAllies(Collections.singletonList(countries1.get(1)));
        Country c = new Country(0l, "PL0");
        c.setAllies(Collections.singletonList(countries2.get(2)));
        countries2.set(0, c);

        Assert.assertFalse(consistencyChecker.checkConsistency(countries1, countries2));

        ArgumentCaptor<String> loggedWarn = ArgumentCaptor.forClass(String.class);
        Mockito.verify(inconsistencyLog, Mockito.times(1)).warn(loggedWarn.capture());

        System.out.println(loggedWarn.getAllValues().get(0));
        // json compare diff: [id=0].allies[allies=null].id,Expected: 1,     got: 2, ; [id=0].allies[allies=null].iso2Code,Expected: PL1,     got: PL2,
        // jiff diff:         0(null != {"iso2Code":"PL0","iso3Code":null,"id":0,"neighbour":null,"allies":[{"iso2Code":"PL2","iso3Code":null,"id":2,"neighbour":null,"allies":null}]}), 0({"iso2Code":"PL0","iso3Code":null,"id":0,"neighbour":null,"allies":[{"iso2Code":"PL1","iso3Code":null,"id":1,"neighbour":null,"allies":null}]} != null)
    }

}
