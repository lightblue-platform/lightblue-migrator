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

/**
 * An abstract base class for Service Facade which handles inconsistencies and logging.
 *
 * @author ykoer
 *
 */
public abstract class AbstractServiceFacade {

    protected static final Logger log = LoggerFactory.getLogger(AbstractServiceFacade.class);
    private static final Logger inconsistencyLog = LoggerFactory.getLogger("Inconsistency");

    // used to associate inconsistencies with the service in the logs
    protected String implementationName;
    protected int maxInconsistencyLogLength = 65536; // 64KB
    protected boolean logData = true;
    private Map<Class<?>,ModelMixIn> modelMixIns;

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

    /* If logData=true:
     *     - Log message < MAX_INCONSISTENCY_LOG_LENGTH to server.log.
     *     - Log message > MAX_INCONSISTENCY_LOG_LENGTH and diff <= MAX_INCONSISTENCY_LOG_LENGTH, log diff to server.log.
     *     - Otherwise log method name and parameters to server.log and full message to inconsistency.log.
     * If logData=false:
     *     - diff <= MAX_INCONSISTENCY_LOG_LENGTH, log diff to server.log.
     *     - Otherwise log method name and parameters to server.log
     *     - Always log full message to inconsistency.log
     */
    private void logInconsistency(String callToLogInCaseOfInconsistency, String legacyJson, String lightblueJson, String diff) {
        String logMessage = String.format("Inconsistency found in %s.%s - diff: %s legacyJson: %s, lightblueJson: %s", implementationName, callToLogInCaseOfInconsistency, diff, legacyJson, lightblueJson);

        if (logData) {
            if (logMessage.length()<=maxInconsistencyLogLength) {
                log.warn(logMessage);
            } else if (diff!=null&&diff.length()<=maxInconsistencyLogLength) {
                log.warn(String.format("Inconsistency found in %s.%s - diff: %s", implementationName, callToLogInCaseOfInconsistency, diff));
            } else {
                log.warn(String.format("Inconsistency found in %s.%s - payload and diff is greater than %d bytes!", implementationName, callToLogInCaseOfInconsistency, maxInconsistencyLogLength));
                inconsistencyLog.debug(logMessage); // logging at debug level since everything >= info would also land in server.log
            }
        } else {
            if (diff!=null&&diff.length()<=maxInconsistencyLogLength) {
                log.warn(String.format("Inconsistency found in %s.%s - diff: %s", implementationName, callToLogInCaseOfInconsistency, diff));
            } else {
                log.warn(String.format("Inconsistency found in %s.%s - diff is greater than %d bytes!", implementationName, callToLogInCaseOfInconsistency, maxInconsistencyLogLength));
            }
            // logData is turned off, log in inconsistency.log for debugging
            inconsistencyLog.debug(logMessage);
        }
    }

    // convenience method for unit testing
    protected boolean checkConsistency(Object o1, Object o2) {
        return checkConsistency(o1, o2, null, null);
    }

    /**
     * Check that objects are equal using org.skyscreamer.jsonassert library.
     *
     * @param o1                              object returned from legacy call
     * @param o2                              object returned from lightblue call
     * @param methodName                      the method name
     * @param callToLogInCaseOfInconsistency  the call including parameters
     * @return
     */
    protected boolean checkConsistency(final Object o1, final Object o2, String methodName, String callToLogInCaseOfInconsistency) {
        if (o1==null&&o2==null) {
            return true;
        }

        String legacyJson=null;
        String lightblueJson=null;
        try {
            long t1 = System.currentTimeMillis();
            legacyJson = getObjectWriter(methodName).writeValueAsString(o1);
            lightblueJson = getObjectWriter(methodName).writeValueAsString(o2);

            JSONCompareResult result = JSONCompare.compareJSON(legacyJson, lightblueJson, JSONCompareMode.LENIENT);
            long t2 = System.currentTimeMillis();

            if (log.isDebugEnabled()) {
                log.debug("Consistency check took: " + (t2-t1)+" ms");
                log.debug("Consistency check passed: "+ result.passed());
            }
            if (!result.passed()) {
                logInconsistency(callToLogInCaseOfInconsistency, legacyJson, lightblueJson, result.getMessage().replaceAll("\n", ","));
            }
            return result.passed();
        } catch (JSONException e) {
            if (o1!=null&&o1.equals(o2)) {
                return true;
            } else {
                logInconsistency(callToLogInCaseOfInconsistency, legacyJson, lightblueJson, null);
            }
        } catch (JsonProcessingException e) {
            log.error("Consistency check failed! Invalid JSON. ", e);
        }
        return false;
    }

    public void setMaxInconsistencyLogLength(int length) {
        this.maxInconsistencyLogLength = length;
    }

    public void setLogData(boolean logData) {
        this.logData = logData;
    }
}
