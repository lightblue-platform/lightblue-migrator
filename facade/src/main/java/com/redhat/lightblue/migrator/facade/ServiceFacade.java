package com.redhat.lightblue.migrator.facade;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStore;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreException;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreImpl;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreSetter;
import com.redhat.lightblue.migrator.features.LightblueMigration;
import com.redhat.lightblue.migrator.features.TogglzRandomUsername;

/**
 * A helper base class for migrating services from legacy datastore to lightblue. It lets you call any service/dao method, using togglz switches to choose which
 * service/dao to use and verifying returned data.
 *
 * @author mpatercz
 *
 */
@SuppressWarnings("all")
public class ServiceFacade<D> implements SharedStoreSetter {

    private static final Logger log = LoggerFactory.getLogger(ServiceFacade.class);
    private static final Logger logInconsisteny = LoggerFactory.getLogger("Inconsistency");

    protected final D legacySvc, lightblueSvc;

    private SharedStore sharedStore = null;

    private Map<Class<?>,ModelMixIn> modelMixIns;

    private int timeoutSeconds = 0;

    private int maxInconsistencyLogLength = 65536; // 64KB

    // used to associate inconsistencies with the service in the logs
    private final String implementationName;

    public SharedStore getSharedStore() {
        return sharedStore;
    }

    public void setSharedStore(SharedStore shareStore) {
        this.sharedStore = shareStore;

        ((SharedStoreSetter)legacySvc).setSharedStore(shareStore);
        ((SharedStoreSetter)lightblueSvc).setSharedStore(shareStore);
    }

    public ServiceFacade(D legacySvc, D lightblueSvc, Class serviceClass) {
        super();
        this.legacySvc = legacySvc;
        this.lightblueSvc = lightblueSvc;
        setSharedStore(new SharedStoreImpl(serviceClass));
        this.implementationName = serviceClass.getSimpleName();
        log.info("Initialized facade for "+implementationName);
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

    // for unit testing
    public boolean checkConsistency(Object o1, Object o2) {
        return checkConsistency(o1, o2, null, null);
    }

    /*
     * Log message < MAX_INCONSISTENCY_LOG_LENGTH to server.log.
     * Log message > MAX_INCONSISTENCY_LOG_LENGTH and diff <= MAX_INCONSISTENCY_LOG_LENGTH, log diff to server.log.
     * Otherwise log method name and parameters to server.log and full message to inconsistency.log.
     */
    private void logInconsistency(String callToLogInCaseOfInconsistency, String legacyJson, String lightblueJson, String diff) {
        String logMessage = String.format("Inconsistency found in %s.%s - diff: %s legacyJson: %s, lightblueJson: %s", implementationName, callToLogInCaseOfInconsistency, diff, legacyJson, lightblueJson);
        if (logMessage.length()<=maxInconsistencyLogLength) {
            log.warn(logMessage);
        } else if (diff!=null&&diff.length()<=maxInconsistencyLogLength) {
            log.warn(String.format("Inconsistency found in %s.%s - diff: %s", implementationName, callToLogInCaseOfInconsistency, diff));
        } else {
            log.warn(String.format("Inconsistency found in %s.%s - payload and diff is greater than %d bytes!", implementationName, callToLogInCaseOfInconsistency, maxInconsistencyLogLength));
            logInconsisteny.debug(logMessage); // logging at debug level since everything >= info would also land in server.log
        }
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
    public boolean checkConsistency(final Object o1, final Object o2, String methodName, String callToLogInCaseOfInconsistency) {
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

    private ListeningExecutorService createExecutor() {
        return MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    }

    private Class[] toClasses(Object[] objects) {
        List<Class> classes = new ArrayList<>();
        for (Object o: objects) {
            classes.add(o.getClass());
        }
        return classes.toArray(new Class[]{});
    }

    /**
     * Return a pretty printed method call.
     *
     * @param methodName
     * @param values
     * @return
     */
    static String methodCallToString(String methodName, Object[] values) {
        try {
            StringBuilder str = new StringBuilder();
            str.append(methodName).append("(");
            Iterator<Object> it = Arrays.asList(values).iterator();
            while(it.hasNext()) {
                Object value = it.next();
                if (value != null && value.getClass().isArray())
                    if (value.getClass().getComponentType().isPrimitive()) {
                        // this is an array of primitives, convert to a meaningful string using reflection
                        String primitiveArrayType = value.getClass().getComponentType().getName();

                        StringBuilder pStr = new StringBuilder();
                        for (int i = 0; i < Array.getLength(value); i ++) {
                            pStr.append(Array.get(value, i));
                            if (i != Array.getLength(value)-1) {
                                pStr.append(", ");
                            }
                        }
                        str.append(primitiveArrayType).append("[").append(pStr.toString()).append("]");
                    }
                    else {
                        str.append(Arrays.deepToString((Object[])value));
                    }
                else
                    str.append(value);
                if (it.hasNext()) {
                    str.append(", ");
                }
            }
            str.append(")");
            return str.toString();
        } catch (Exception e) {
            log.error("Creating method call string failed", e);
            return "<creating method call string failed>";
        }
    }

    private <T> ListenableFuture<T> callLightblueSvc(final boolean passIds, final Method method, final Object[] values) {
        ListeningExecutorService executor = createExecutor();
        try {
        // fetch from lightblue using future (asynchronously)
        final long parentThreadId = Thread.currentThread().getId();
        return executor.submit(new Callable<T>(){
            @Override
            public T call() throws Exception {
                Timer dest = new Timer("destination."+method.getName());
                if (sharedStore != null && passIds)
                    sharedStore.copyFromThread(parentThreadId);
                try {
                    return (T) method.invoke(lightblueSvc, values);
                } finally {
                    dest.complete();
                }
            }
        });
        } finally {
            executor.shutdown();
        }
    }

    private <T> T getWithTimeout(ListenableFuture<T> listenableFuture) throws InterruptedException, ExecutionException, TimeoutException {
        if (timeoutSeconds <= 0) {
            return listenableFuture.get();
        } else {
            return listenableFuture.get(timeoutSeconds, TimeUnit.SECONDS);
        }
    }

    private Throwable extractUnderlyingException(Throwable t) {
        if ((t instanceof ExecutionException || t instanceof InvocationTargetException) && t.getCause() != null) {
            return extractUnderlyingException(t.getCause());
        }
        else {
            return t;
        }
    }

    // which togglz flags to use
    public enum FacadeOperation {
        READ, WRITE;
    }

    private boolean shouldDestination(FacadeOperation facadeOperation) {
        switch (facadeOperation) {
            case READ: return LightblueMigration.shouldReadDestinationEntity();
            case WRITE: return LightblueMigration.shouldWriteDestinationEntity();
            default: throw new IllegalArgumentException(facadeOperation.toString());
        }
    }

    private boolean shouldSource(FacadeOperation facadeOperation) {
        switch (facadeOperation) {
            case READ: return LightblueMigration.shouldReadSourceEntity();
            case WRITE: return LightblueMigration.shouldWriteSourceEntity();
            default: throw new IllegalArgumentException(facadeOperation.toString());
        }
    }

    private boolean shouldCheckConsistencyConsistency(FacadeOperation facadeOperation) {
        switch (facadeOperation) {
            case READ: return LightblueMigration.shouldCheckReadConsistency();
            case WRITE: return LightblueMigration.shouldCheckWriteConsistency();
            default: throw new IllegalArgumentException(facadeOperation.toString());
        }
    }

    /**
     * Call service method according to settings specified in togglz.
     *
     * @param facadeOperation READ/WRITE operation, used by togglz flags
     * @param callInParallel true means both oracle and lightblue will be called in parallel and state sharing between services will not be possible
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param types List of parameter types
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callSvcMethod(final FacadeOperation facadeOperation, final boolean callInParallel, final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Throwable {
        if (log.isDebugEnabled())
            log.debug("Performing parallel="+callInParallel+" "+facadeOperation+" "+(returnedType!=null?returnedType.getName():"")+" "+methodCallToString(methodName, values));

        TogglzRandomUsername.init();

        if (sharedStore != null && shouldSource(facadeOperation) && shouldDestination(facadeOperation)) {
            sharedStore.setDualMigrationPhase(true);
        } else if (sharedStore != null){
            sharedStore.setDualMigrationPhase(false);
        }

        T legacyEntity = null, lightblueEntity = null;

        ListenableFuture<T> listenableFuture = null;

        if (callInParallel) {
            if (shouldDestination(facadeOperation)) {
                Method method = lightblueSvc.getClass().getMethod(methodName, types);
                listenableFuture = callLightblueSvc(false, method, values);
            }
        }

        if (shouldSource(facadeOperation)) {
            // perform operation in oracle, synchronously
            log.debug("."+methodName+" creating in legacy");
            Method method = legacySvc.getClass().getMethod(methodName,types);
            Timer source = new Timer("source."+methodName);
            try {
                legacyEntity = (T) method.invoke(legacySvc, values);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } finally {
                source.complete();
            }
        }

        if (shouldDestination(facadeOperation)) {
            log.debug("."+methodName+" "+facadeOperation+" in lightblue");

            try {
                if (callInParallel) {
                    // lightblue was called before source/legacy, now just getting results
                    lightblueEntity = getWithTimeout(listenableFuture);
                } else {
                    // call lightblue after source/legacy finished
                    Method method = lightblueSvc.getClass().getMethod(methodName, types);
                    listenableFuture = callLightblueSvc(true, method, values);
                    lightblueEntity = getWithTimeout(listenableFuture);
                }
            } catch (TimeoutException te) {
                if (shouldSource(facadeOperation)) {
                    log.warn("Lightblue call is taking too long (longer than "+timeoutSeconds+"s). Returning data from legacy.", te);
                    return legacyEntity;
                } else {
                    throw te;
                }
            } catch (Throwable e) {
                if (shouldSource(facadeOperation)) {
                    log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                    return legacyEntity;
                } else {
                    throw extractUnderlyingException(e);
                }
            }
        }

        if (shouldCheckConsistencyConsistency(facadeOperation) && shouldSource(facadeOperation) && shouldDestination(facadeOperation)) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");

            // check if entities match
            if (checkConsistency(lightblueEntity, legacyEntity, methodName, methodCallToString(methodName, values))) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                return legacyEntity;
            }
        }

        return lightblueEntity != null ? lightblueEntity : legacyEntity;
    }

    private SharedStoreException extractSharedStoreExceptionIfExists(ExecutionException ee) {
        try {
            if (ee.getCause().getCause() instanceof SharedStoreException) {
                return (SharedStoreException)ee.getCause().getCause();
            } else {
                return null;
            }
        } catch (NullPointerException e) {
            return null;
        }
    }

    public <T> T callSvcWriteMethod(boolean callInParallel, final Class<T> returnedType, final String methodName, final Object ... values) throws Throwable {
        return callSvcWriteMethod(callInParallel, returnedType, methodName, toClasses(values), values);
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public void setMaxInconsistencyLogLength(int length) {
        this.maxInconsistencyLogLength = length;
    }

    public int getMaxInconsistencyLogLength() {
        return maxInconsistencyLogLength;
    }

    public D getLegacySvc() {
        return legacySvc;
    }

}
