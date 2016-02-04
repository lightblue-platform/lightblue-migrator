package com.redhat.lightblue.migrator.facade;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.reflections.Reflections;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redhat.lightblue.migrator.facade.methodcallstringifier.LazyMethodCallStringifier;
import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;

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

    public ConsistencyChecker(String implementationName) {
        this.implementationName = implementationName;
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

    private ObjectWriter getObjectWriter(String methodName) {
        ObjectMapper mapper = new ObjectMapper();
        for (Map.Entry<Class<?>, ModelMixIn> entry : findModelMixInMappings().entrySet()) {
            if (methodName==null || entry.getValue().includeMethods().length==0 || Arrays.asList(entry.getValue().includeMethods()).contains(methodName)) {
                mapper.addMixInAnnotations(entry.getValue().clazz(), entry.getKey());
            }
        }
        return mapper.writer();
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
    private void logInconsistency(String callToLogInCaseOfInconsistency, String legacyJson, String lightblueJson, String diff) {
        String logMessage = String.format("Inconsistency found in %s.%s - diff: %s - legacyJson: %s, lightblueJson: %s", implementationName, callToLogInCaseOfInconsistency, diff, legacyJson, lightblueJson);
        if (logResponseDataEnabled) {
            if (logMessage.length()<=maxInconsistencyLogLength) {
                inconsistencyLog.warn(logMessage);
            } else if (diff!=null&&diff.length()<=maxInconsistencyLogLength) {
                inconsistencyLog.warn(String.format("Inconsistency found in %s.%s - diff: %s", implementationName, callToLogInCaseOfInconsistency, diff));
                hugeInconsistencyLog.debug(logMessage);
            } else {
                inconsistencyLog.warn(String.format("Inconsistency found in %s.%s - payload and diff is greater than %d bytes!", implementationName, callToLogInCaseOfInconsistency, maxInconsistencyLogLength));
                hugeInconsistencyLog.debug(logMessage);
            }
        } else {
            if (diff!=null&&diff.length()<=maxInconsistencyLogLength) {
                inconsistencyLog.warn(String.format("Inconsistency found in %s.%s - diff: %s", implementationName, callToLogInCaseOfInconsistency, diff));
            } else {
                inconsistencyLog.warn(String.format("Inconsistency found in %s.%s - diff is greater than %d bytes!", implementationName, callToLogInCaseOfInconsistency, maxInconsistencyLogLength));
            }
            // logData is turned off, log in inconsistency.log for debugging
            hugeInconsistencyLog.debug(logMessage);
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
    protected boolean checkConsistency(final Object legacyEntity, final Object lightblueEntity, String methodName, MethodCallStringifier callToLogInCaseOfInconsistency) {
        if (legacyEntity==null&&lightblueEntity==null) {
            return true;
        }

        String legacyJson=null;
        String lightblueJson=null;

        if (callToLogInCaseOfInconsistency == null)
            callToLogInCaseOfInconsistency = new LazyMethodCallStringifier();

        try {
            long t1 = System.currentTimeMillis();
            legacyJson = getObjectWriter(methodName).writeValueAsString(legacyEntity);
            lightblueJson = getObjectWriter(methodName).writeValueAsString(lightblueEntity);

            JSONCompareResult result = JSONCompare.compareJSON(legacyJson, lightblueJson, JSONCompareMode.LENIENT);
            long t2 = System.currentTimeMillis();

            if (inconsistencyLog.isDebugEnabled()) {
                inconsistencyLog.debug("Consistency check took: " + (t2-t1)+" ms");
                inconsistencyLog.debug("Consistency check passed: "+ result.passed());
            }
            if (!result.passed()) {
                logInconsistency(callToLogInCaseOfInconsistency.toString(), legacyJson, lightblueJson, result.getMessage().replaceAll("\n", ","));
            }
            return result.passed();
        } catch (JSONException e) {
            if (legacyEntity!=null&&legacyEntity.equals(lightblueEntity)) {
                return true;
            } else {
                logInconsistency(callToLogInCaseOfInconsistency.toString(), legacyJson, lightblueJson, null);
            }
        } catch (JsonProcessingException e) {
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
