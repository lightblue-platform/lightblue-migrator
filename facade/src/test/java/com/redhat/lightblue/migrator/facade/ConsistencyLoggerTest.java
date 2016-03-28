package com.redhat.lightblue.migrator.facade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redhat.lightblue.migrator.facade.methodcallstringifier.LazyMethodCallStringifier;

@RunWith(MockitoJUnitRunner.class)
public class ConsistencyLoggerTest {

    @Mock
    private Logger inconsistencyLog;

    @Mock
    private Logger hugeInconsistencyLog;

    @InjectMocks
    private ConsistencyChecker consistencyChecker = new ConsistencyChecker(CountryDAO.class.getSimpleName());

    @Captor
    private ArgumentCaptor<String> logStmt;

    @Captor
    private ArgumentCaptor<String> inconsistencyLogStmt;

    @Before
    public void setup() throws InstantiationException, IllegalAccessException {
        initMocks(this);
    }

    @After
    public void after() {
        // no errors logged to inconsistency log
        Mockito.verify(inconsistencyLog, Mockito.never()).error(Mockito.anyString());
    }

    private List<String> getDummyMessage(String prefix, int maxBytes) throws JsonProcessingException {

        int count = maxBytes / 12; // one country is ~12 bytes when its transformed to json

        List<String> list = new ArrayList<>();
        for (long i=0;i<count;i++) {
            list.add(String.valueOf(prefix+(i+10000)));
        }

        return list;
    }

    @Test
    public void logTest_logResponsesEnabled_logLessThanLogLimit() throws Exception {

        // set maxLogLength to simplify unit testing
        consistencyChecker.setMaxInconsistencyLogLength(177);

        // enable response data logging
        consistencyChecker.setLogResponseDataEnabled(true);

        List<String> legacy = getDummyMessage("Test", 24);
        List<String> lightblue = getDummyMessage("Test", 12);
        boolean result = consistencyChecker.checkConsistency(legacy, lightblue, "testMethod", new LazyMethodCallStringifier("testMethod(param1, param2)"));
        assertFalse(result);
        verify(inconsistencyLog).warn(logStmt.capture());
        assertEquals("[main] Inconsistency found in CountryDAO.testMethod(param1, param2) - diff: []: Expected 2 values but got 1 - legacyJson: [\"Test10000\",\"Test10001\"], lightblueJson: [\"Test10000\"]", logStmt.getValue());
        verify(hugeInconsistencyLog, never()).debug(anyString());
    }

    @Test
    public void logTest_logResponsesEnabled_logGreaterThanLogLimit() throws Exception {

        // set maxLogLength to simplify unit testing
        consistencyChecker.setMaxInconsistencyLogLength(177);

        // enable response data logging
        consistencyChecker.setLogResponseDataEnabled(true);

        List<String> legacy = getDummyMessage("Test", 36);
        List<String> lightblue = getDummyMessage("Test", 24);
        boolean result = consistencyChecker.checkConsistency(legacy, lightblue, "testMethod", new LazyMethodCallStringifier("testMethod(param1, param2)"));
        assertFalse(result);
        verify(inconsistencyLog).warn(logStmt.capture());
        assertEquals("[main] Inconsistency found in CountryDAO.testMethod(param1, param2) - diff: []: Expected 3 values but got 2", logStmt.getValue());
        verify(hugeInconsistencyLog).debug(inconsistencyLogStmt.capture());
        assertEquals("[main] Inconsistency found in CountryDAO.testMethod(param1, param2) - diff: []: Expected 3 values but got 2 - legacyJson: [\"Test10000\",\"Test10001\",\"Test10002\"], lightblueJson: [\"Test10000\",\"Test10001\"]", inconsistencyLogStmt.getValue());
    }

    @Test
    public void logTest_logResponsesEnabled_diffGreaterThanLogLimit() throws Exception {

        // set maxLogLength to simplify unit testing
        consistencyChecker.setMaxInconsistencyLogLength(177);

        // enable response data logging
        consistencyChecker.setLogResponseDataEnabled(true);

        List<String> legacy = getDummyMessage("Test", 40);
        List<String> lightblue = getDummyMessage("Fooo", 40);
        boolean result = consistencyChecker.checkConsistency(legacy, lightblue, "testMethod", new LazyMethodCallStringifier("testMethod(param1, param2)"));
        assertFalse(result);
        verify(inconsistencyLog).warn(logStmt.capture());
        assertEquals("[main] Inconsistency found in CountryDAO.testMethod(param1, param2) - payload and diff is greater than 177 bytes!", logStmt.getValue());
        verify(hugeInconsistencyLog).debug(inconsistencyLogStmt.capture());
        assertTrue(inconsistencyLogStmt.getValue().contains("diff"));
        assertTrue(inconsistencyLogStmt.getValue().contains("legacyJson"));
        assertTrue(inconsistencyLogStmt.getValue().contains("lightblueJson"));
    }

    @Test
    public void logTest_logResponsesDisabled_logLessThanLogLimit() throws Exception {

        // set maxLogLength to simplify unit testing
        consistencyChecker.setMaxInconsistencyLogLength(177);

        // disable response data logging
        consistencyChecker.setLogResponseDataEnabled(false);

        List<String> legacy = getDummyMessage("Test", 24);
        List<String> lightblue = getDummyMessage("Test", 12);
        boolean result = consistencyChecker.checkConsistency(legacy, lightblue, "testMethod", new LazyMethodCallStringifier("testMethod(param1, param2)"));
        assertFalse(result);
        verify(inconsistencyLog).warn(logStmt.capture());
        assertEquals("[main] Inconsistency found in CountryDAO.testMethod(param1, param2) - diff: []: Expected 2 values but got 1", logStmt.getValue());
        verify(hugeInconsistencyLog).debug(inconsistencyLogStmt.capture());
        assertEquals("[main] Inconsistency found in CountryDAO.testMethod(param1, param2) - diff: []: Expected 2 values but got 1 - legacyJson: [\"Test10000\",\"Test10001\"], lightblueJson: [\"Test10000\"]", inconsistencyLogStmt.getValue());
    }

    @Test
    public void logTest_logResponsesDisabled_diffGreaterThanLogLimit() throws Exception {

        // set maxLogLength to simplify unit testing
        consistencyChecker.setMaxInconsistencyLogLength(177);

        // disable response data logging
        consistencyChecker.setLogResponseDataEnabled(false);

        List<String> legacy = getDummyMessage("Test", 40);
        List<String> lightblue = getDummyMessage("Fooo", 40);
        boolean result = consistencyChecker.checkConsistency(legacy, lightblue, "testMethod", new LazyMethodCallStringifier("testMethod(param1, param2)"));
        assertFalse(result);
        verify(inconsistencyLog).warn(logStmt.capture());
        assertEquals("[main] Inconsistency found in CountryDAO.testMethod(param1, param2) - diff is greater than 177 bytes!", logStmt.getValue());
        verify(hugeInconsistencyLog).debug(inconsistencyLogStmt.capture());
        assertTrue(inconsistencyLogStmt.getValue().contains("diff"));
        assertTrue(inconsistencyLogStmt.getValue().contains("legacyJson"));
        assertTrue(inconsistencyLogStmt.getValue().contains("lightblueJson"));
    }

    /**
     * Changing logging category can break logging if relevant log4j configs on the application side
     * are not updated as well. This regression test ensures that logging category is not accidently changed.
     *
     */
    @Test
    public void testLoggerName() {
        ConsistencyChecker c = new ConsistencyChecker("implName");
        Assert.assertEquals("com.redhat.lightblue.migrator.facade.ConsistencyChecker", c.inconsistencyLog.getName());
        Assert.assertEquals("com.redhat.lightblue.migrator.facade.ConsistencyCheckerHuge", c.hugeInconsistencyLog.getName());
    }
}
