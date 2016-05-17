package com.redhat.lightblue.migrator.facade;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;
import com.redhat.lightblue.migrator.test.LightblueMigrationPhase;

/**
 * Test thread pool for lightblue calls.
 *
 * @author mpatercz
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ServiceFacadeThreadPoolTest {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    @Mock
    CountryDAOFacadable legacyDAO;
    @Mock
    CountryDAOFacadable lightblueDAO;

    CountryDAO countryDAOProxy;

    ServiceFacade<CountryDAOFacadable> daoFacade;

    @Before
    public void setup() throws InstantiationException, IllegalAccessException, CountryException {
        Mockito.when(lightblueDAO.getCountry(Mockito.any(String.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(200);
                return new Country("pl");
            }

        });

        Mockito.when(legacyDAO.getCountry(Mockito.any(String.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                return new Country("pl");
            }

        });

        Mockito.when(lightblueDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(200);
                return new Country("pl");
            }

        });

        Mockito.when(legacyDAO.createCountry(Mockito.any(Country.class))).thenAnswer(new Answer<Country>() {

            @Override
            public Country answer(InvocationOnMock invocation) throws Throwable {
                return new Country("pl");
            }

        });

        Properties p = new Properties();
        // only 2 parallel calls to lightblue are allowed
        p.setProperty(ServiceFacade.CONFIG_PREFIX+"CountryDAOFacade.threadPool.size", "2");
        daoFacade = new ServiceFacade<CountryDAOFacadable>(legacyDAO, lightblueDAO, "CountryDAOFacade", p);

        Mockito.verify(legacyDAO).setSharedStore((daoFacade).getSharedStore());
        Mockito.verify(lightblueDAO).setSharedStore((daoFacade).getSharedStore());

        // countryDAOProxy is daoFacade using CountryDAO interface to invoke methods
        countryDAOProxy = FacadeProxyFactory.createFacadeProxy(daoFacade, CountryDAOFacadable.class);
    }

    Callable<Country> getCountryCallable = new Callable<Country>() {

        @Override
        public Country call() throws Exception {
            return countryDAOProxy.getCountry("pl");
        }

    };

    Callable<Country> createCountryCallable = new Callable<Country>() {

        @Override
        public Country call() throws Exception {
            return countryDAOProxy.createCountry(new Country("pl"));
        }

    };

    @Test
    public void threadPoolTest_read_dualPhase() throws InstantiationException, IllegalAccessException, CountryException, InterruptedException, ExecutionException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        ExecutorService executor = Executors.newFixedThreadPool(10);

        // calling 4x getCountry in parallel
        Future<Country> c1 = executor.submit(getCountryCallable);
        Future<Country> c2 = executor.submit(getCountryCallable);
        Future<Country> c3 = executor.submit(getCountryCallable);
        Future<Country> c4 = executor.submit(getCountryCallable);

        // make sure client does not see any errors
        Assert.assertEquals(new Country("pl"), c1.get());
        Assert.assertEquals(new Country("pl"), c2.get());
        Assert.assertEquals(new Country("pl"), c3.get());
        Assert.assertEquals(new Country("pl"), c4.get());

        // lightblueDAO was called only 2 times, because that's the pool size
        Mockito.verify(lightblueDAO, Mockito.times(2)).getCountry("pl");
        // legacyDAO was called 4 times, which matches the number of facade calls
        Mockito.verify(legacyDAO, Mockito.times(4)).getCountry("pl");

        executor.shutdown();
    }

    @Test
    public void threadPoolTest_write_dualPhase() throws InstantiationException, IllegalAccessException, CountryException, InterruptedException, ExecutionException {
        LightblueMigrationPhase.dualReadPhase(togglzRule);

        ExecutorService executor = Executors.newFixedThreadPool(10);

        // calling 4x createCountry in parallel
        Future<Country> c1 = executor.submit(createCountryCallable);
        Future<Country> c2 = executor.submit(createCountryCallable);
        Future<Country> c3 = executor.submit(createCountryCallable);
        Future<Country> c4 = executor.submit(createCountryCallable);

        // make sure client does not see any errors
        Assert.assertEquals(new Country("pl"), c1.get());
        Assert.assertEquals(new Country("pl"), c2.get());
        Assert.assertEquals(new Country("pl"), c3.get());
        Assert.assertEquals(new Country("pl"), c4.get());

        // lightblueDAO was called only 2 times, because that's the pool size
        Mockito.verify(lightblueDAO, Mockito.times(2)).createCountry(new Country("pl"));
        // legacyDAO was called 4 times, which matches the number of facade calls
        Mockito.verify(legacyDAO, Mockito.times(4)).createCountry(new Country("pl"));

        executor.shutdown();
    }

    // no fallback
    @Test
    public void threadPoolTest_proxyPhase() throws InstantiationException, IllegalAccessException, CountryException, InterruptedException, ExecutionException {
        LightblueMigrationPhase.lightblueProxyPhase(togglzRule);

        ExecutorService executor = Executors.newFixedThreadPool(10);

        // calling 3x getCountry in parallel
        Future<Country> c1 = executor.submit(getCountryCallable);
        Future<Country> c2 = executor.submit(getCountryCallable);
        Future<Country> c3 = executor.submit(getCountryCallable);


        int exceptionCount = 0;
        try {
            c1.get();
        } catch (ExecutionException e) {
            exceptionCount++;
        }
        try {
            c2.get();
        } catch (ExecutionException e) {
            exceptionCount++;
        }
        try {
            c3.get();
        } catch (ExecutionException e) {
            exceptionCount++;
        }

        // there was one exception
        Assert.assertEquals(1, exceptionCount);

        // lightblueDAO was called only 2 times, because that's the pool size
        Mockito.verify(lightblueDAO, Mockito.times(2)).getCountry("pl");

        executor.shutdown();
    }

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(lightblueDAO);
        Mockito.verifyNoMoreInteractions(legacyDAO);

        daoFacade.shutdown();
    }

}
