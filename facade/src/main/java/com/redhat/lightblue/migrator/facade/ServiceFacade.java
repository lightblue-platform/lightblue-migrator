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
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Properties properties = new Properties();

    private TimeoutConfiguration timeoutConfiguration;

    // default timeout is 2 seconds
    public static final long DEFAULT_TIMEOUT_MS = 2000;

    private ConsistencyChecker consistencyChecker;

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

    public ConsistencyChecker getConsistencyChecker() {
        if (consistencyChecker == null) {
            consistencyChecker = new ConsistencyChecker(implementationName);
        }
        return consistencyChecker;
    }

    public void setConsistencyChecker(ConsistencyChecker consistencyChecker) {
        this.consistencyChecker = consistencyChecker;
    }

    public ServiceFacade(D legacySvc, D lightblueSvc, Class serviceClass) {
        this(legacySvc, lightblueSvc, serviceClass, null);
    }

    public ServiceFacade(D legacySvc, D lightblueSvc, Class serviceClass, Properties properties) {
        super();
        this.legacySvc = legacySvc;
        this.lightblueSvc = lightblueSvc;
        setSharedStore(new SharedStoreImpl(serviceClass));
        this.implementationName = serviceClass.getSimpleName();
        if (properties != null)
            this.properties = properties;

        timeoutConfiguration = new TimeoutConfiguration(DEFAULT_TIMEOUT_MS, implementationName, this.properties);

        log.info("Initialized facade for "+implementationName);
    }

    private long getLightblueExecutionTimeout(String methodName) {


        String timeout = properties.getProperty("com.redhat.lightblue.migrator.facade.timeout."+implementationName+"."+methodName);
        if (timeout == null)
            timeout = properties.getProperty("com.redhat.lightblue.migrator.facade.timeout."+implementationName, ""+DEFAULT_TIMEOUT_MS);

        return Long.parseLong(timeout);
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

    private <T> T getWithTimeout(ListenableFuture<T> listenableFuture, String methodName) throws InterruptedException, ExecutionException, TimeoutException {
        if (timeoutConfiguration.getTimeoutMS(methodName) <= 0) {
            return listenableFuture.get();
        } else {
            return listenableFuture.get(timeoutConfiguration.getTimeoutMS(methodName), TimeUnit.MILLISECONDS);
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
        if (log.isDebugEnabled()) {
            log.debug("Calling {}.{} ({} {})", implementationName, methodCallToString(methodName, values), callInParallel ? "parallel": "serial", facadeOperation);
        }

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
            log.debug("Calling legacy {}.{}", implementationName, methodName);
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
            log.debug("Calling lightblue {}.{}", implementationName, methodName);

            try {
                if (callInParallel) {
                    // lightblue was called before source/legacy, now just getting results
                    lightblueEntity = getWithTimeout(listenableFuture, methodName);
                } else {
                    // call lightblue after source/legacy finished
                    Method method = lightblueSvc.getClass().getMethod(methodName, types);
                    listenableFuture = callLightblueSvc(true, method, values);
                    lightblueEntity = getWithTimeout(listenableFuture, methodName);
                }
            } catch (TimeoutException te) {
                if (shouldSource(facadeOperation)) {
                    log.warn("Lightblue call "+implementationName+"."+methodCallToString(methodName, values)+" is taking too long (longer than "+timeoutConfiguration.getTimeoutMS(methodName)+"s). Returning data from legacy.", te);
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
            if (getConsistencyChecker().checkConsistency(legacyEntity, lightblueEntity, methodName, methodCallToString(methodName, values))) {
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

    @Deprecated
    public int getTimeoutSeconds() {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public void setTimeoutSeconds(int timeoutSeconds) {
        timeoutConfiguration = new TimeoutConfiguration(timeoutSeconds*1000, implementationName, properties);
    }

    public void setLogResponseDataEnabled(boolean logResponsesEnabled) {
        getConsistencyChecker().setLogResponseDataEnabled(logResponsesEnabled);
    }

    public void setMaxInconsistencyLogLength(int length) {
        getConsistencyChecker().setMaxInconsistencyLogLength(length);
    }

    public D getLegacySvc() {
        return legacySvc;
    }

    public TimeoutConfiguration getTimeoutConfiguration() {
        return timeoutConfiguration;
    }

    public void setTimeoutConfiguration(TimeoutConfiguration timeoutConfiguration) {
        this.timeoutConfiguration = timeoutConfiguration;
    }
}
