package com.redhat.lightblue.migrator.facade;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redhat.lightblue.migrator.facade.methodcallstringifier.LazyMethodCallStringifier;
import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;

import jiff.JsonDiff;

/**
 * This class checks for data inconsistencies and handles logging.
 *
 * @author ykoer
 *
 */
public class ConsistencyChecker {

    // using non-static slf4j loggers for easy unit testing
    private Logger inconsistencyLog = LoggerFactory.getLogger(this.getClass());
    private Logger hugeInconsistencyLog = LoggerFactory.getLogger(this.getClass()+"Huge");

    private final String implementationName;
    protected int maxInconsistencyLogLength = 65536; // 64KB
    protected boolean logResponseDataEnabled = true;
    private Map<Class<?>,ModelMixIn> modelMixIns;
    private ObjectMapper objectMapper = null;

    private JsonDiff jiff = new JsonDiff();

    public ConsistencyChecker(String implementationName) {
        this.implementationName = implementationName;
        jiff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);

        this.objectMapper = createObjectMapper();
    }

    private Map<Class<?>,ModelMixIn> findModelMixInMappings() {
        if  (modelMixIns==null) {
            Reflections reflections = new Reflections("");
            Set<Class<?>> classes = reflections.getTypesAnnotatedWith(ModelMixIn.class);
            modelMixIns = new HashMap<>();
            for (Class<?> clazz : classes) {
                modelMixIns.put(clazz, clazz.getAnnotation(ModelMixIn.class));
            }
        }
        return modelMixIns;
    }

    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        for (Map.Entry<Class<?>, ModelMixIn> entry : findModelMixInMappings().entrySet()) {
            mapper.addMixIn(entry.getValue().clazz(), entry.getKey());
        }
        return mapper;
    }

    /* Log inconsistencies based on following rules:
     *
     * If logResponseDataEnabled=true:
     *     - Log message < MAX_INCONSISTENCY_LOG_LENGTH to server.log.
     *     - Log message > MAX_INCONSISTENCY_LOG_LENGTH and diff <= MAX_INCONSISTENCY_LOG_LENGTH, log diff to server.log.
     *     - Otherwise log method name and parameters to server.log and full message to inconsistency.log.
     * If logResponseDataEnabled=false:
     *     - diff <= MAX_INCONSISTENCY_LOG_LENGTH, log diff to server.log.
     *     - Otherwise log method name and parameters to server.log
     *     - Always log full message to inconsistency.log
     *
     *  Logging inconsistencies at debug level since everything >= info would also appear in server.log
     */
    private void logInconsistency(String parentThreadName, String callToLogInCaseOfInconsistency, String legacyJson, String lightblueJson, String diff) {
        String logMessage = String.format("[%s] Inconsistency found in %s.%s - diff: %s - legacyJson: %s, lightblueJson: %s", parentThreadName, implementationName, callToLogInCaseOfInconsistency, diff, legacyJson, lightblueJson);
        if (logResponseDataEnabled) {
            if (logMessage.length()<=maxInconsistencyLogLength) {
                inconsistencyLog.warn(logMessage);
            } else if (diff!=null&&diff.length()<=maxInconsistencyLogLength) {
                inconsistencyLog.warn(String.format("[%s] Inconsistency found in %s.%s - diff: %s", parentThreadName, implementationName, callToLogInCaseOfInconsistency, diff));
                hugeInconsistencyLog.debug(logMessage);
            } else {
                inconsistencyLog.warn(String.format("[%s] Inconsistency found in %s.%s - payload and diff is greater than %d bytes!", parentThreadName, implementationName, callToLogInCaseOfInconsistency, maxInconsistencyLogLength));
                hugeInconsistencyLog.debug(logMessage);
            }
        } else {
            if (diff!=null&&diff.length()<=maxInconsistencyLogLength) {
                inconsistencyLog.warn(String.format("[%s] Inconsistency found in %s.%s - diff: %s", parentThreadName, implementationName, callToLogInCaseOfInconsistency, diff));
            } else {
                inconsistencyLog.warn(String.format("[%s] Inconsistency found in %s.%s - diff is greater than %d bytes!", parentThreadName, implementationName, callToLogInCaseOfInconsistency, maxInconsistencyLogLength));
            }
            // logData is turned off, log in inconsistency.log for debugging
            hugeInconsistencyLog.debug(logMessage);
        }
    }


    private void logInconsistencyUsingJSONCompare(final String parentThreadName, final String legacyJson, final String lightblueJson, final MethodCallStringifier callToLogInCaseOfInconsistency) {
        try {
            Timer t = new Timer("ConsistencyCheck (JSONCompare)");

            JSONCompareResult result = JSONCompare.compareJSON(legacyJson, lightblueJson, JSONCompareMode.NON_EXTENSIBLE);

            long jiffConsistencyCheckTook = t.complete();

            if (inconsistencyLog.isDebugEnabled()) {
                inconsistencyLog.debug(String.format("[%s] SONCompare consistency check took: %dms", parentThreadName, jiffConsistencyCheckTook));
                inconsistencyLog.debug(String.format("[%s] JSONCompare consistency check passed: true", parentThreadName));
            }

            if (result.passed()) {
                inconsistencyLog.error(String.format("[%s] Jiff consistency check found an inconsistency but JSONCompare didn't! Happened in %s", parentThreadName, callToLogInCaseOfInconsistency.toString()));
                return;
            }

            // log nice diff
            logInconsistency(parentThreadName, callToLogInCaseOfInconsistency.toString(), legacyJson, lightblueJson, result.getMessage().replaceAll("\n", ","));
        } catch (Exception e) {
            inconsistencyLog.error("JSONCompare consistency check failed for "+callToLogInCaseOfInconsistency, e);
        }
    }

    private void logInconsistencyUsingJSONCompare(final String parentThreadName, final String legacyJson, final String lightblueJson, final MethodCallStringifier callToLogInCaseOfInconsistency, boolean blocking) {
        if (blocking) {
            inconsistencyLog.debug("Running logInconsistencyUsingJSONCompare in a blocking manner");
            logInconsistencyUsingJSONCompare(parentThreadName, legacyJson, lightblueJson, callToLogInCaseOfInconsistency);
        } else {
            inconsistencyLog.debug("Running logInconsistencyUsingJSONCompare in a NON blocking manner");
            new Thread(new Runnable() {

                @Override
                public void run() {
                    logInconsistencyUsingJSONCompare(parentThreadName, legacyJson, lightblueJson, callToLogInCaseOfInconsistency);
                }
            }).start();
        }
    }

    // convenience method for unit testing
    protected boolean checkConsistency(Object o1, Object o2) {
        return checkConsistency(o1, o2, null, null);
    }

    /**
     * Check that objects are equal using org.skyscreamer.jsonassert library.
     *
     * @param legacyEntity                    object returned from legacy call
     * @param lightblueEntity                 object returned from lightblue call
     * @param methodName                      the method name
     * @param callToLogInCaseOfInconsistency  the call including parameters
     * @return
     */
    public boolean checkConsistency(final Object legacyEntity, final Object lightblueEntity, final String methodName, MethodCallStringifier callToLogInCaseOfInconsistency) {

        Timer ti = new Timer("initial");
        if (legacyEntity==null&&lightblueEntity==null) {
            return true;
        }

        if (callToLogInCaseOfInconsistency == null) {
            callToLogInCaseOfInconsistency = new LazyMethodCallStringifier();
        }

        Timer p2j = new Timer("ConsistencyChecker's pojo2json conversion");
        final JsonNode legacyJson = objectMapper.valueToTree(legacyEntity);
        final JsonNode lightblueJson = objectMapper.valueToTree(lightblueEntity);
        p2j.complete();

        try {
            Timer t = new Timer("checkConsistency (jiff)");

            boolean consistent = jiff.computeDiff(legacyJson, lightblueJson).isEmpty();

            long jiffConsistencyCheckTook = t.complete();

            if (inconsistencyLog.isDebugEnabled()) {
                inconsistencyLog.debug("Jiff consistency check took: " + jiffConsistencyCheckTook+" ms");
                inconsistencyLog.debug("Jiff consistency check passed: true");
            }

            if (consistent) {
                return true;
            }

            // objects are inconsistent, use org.skyscreamer.jsonassert.JSONCompare to produce inconsistency warning
            // it's slow, but produces nice diffs
            String legacyJsonStr =  objectMapper.writeValueAsString(legacyEntity);
            String lightblueJsonStr =  objectMapper.writeValueAsString(lightblueEntity);
            logInconsistencyUsingJSONCompare(Thread.currentThread().getName(), legacyJsonStr, lightblueJsonStr, callToLogInCaseOfInconsistency, Boolean.valueOf(System.getProperty("lightblue.facade.consistencyChecker.blocking", "false")));

            // inconsistent
            return false;

        } catch (IOException e) {
            inconsistencyLog.error("Consistency check failed in "+implementationName+"."+callToLogInCaseOfInconsistency+"! Invalid JSON: legacyJson="+legacyJson+", lightblueJson="+lightblueJson, e);
        }
        return false;
    }

    public void setMaxInconsistencyLogLength(int length) {
        this.maxInconsistencyLogLength = length;
    }

    public void setLogResponseDataEnabled(boolean logResponseDataEnabled) {
        this.logResponseDataEnabled = logResponseDataEnabled;
    }
}
