package com.redhat.lightblue.migrator.facade;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
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
import com.redhat.lightblue.migrator.facade.methodcallstringifier.LazyMethodCallStringifier;
import com.redhat.lightblue.migrator.facade.methodcallstringifier.MethodCallStringifier;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStore;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreException;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreImpl;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreSetter;
import com.redhat.lightblue.migrator.features.LightblueMigration;
import com.redhat.lightblue.migrator.features.TogglzRandomUsername;

/**
 * A helper base class for migrating services from legacy datastore to
 * lightblue. It lets you call any service/dao method, using togglz switches to
 * choose which service/dao to use and verifying returned data.
 *
 * @author mpatercz
 *
 */
@SuppressWarnings("all")
public class ServiceFacade<D extends SharedStoreSetter> implements SharedStoreSetter {

    private static final Logger log = LoggerFactory.getLogger(ServiceFacade.class);

    protected final D legacySvc, lightblueSvc;

    private SharedStore sharedStore = null;

    private Map<Class<?>, ModelMixIn> modelMixIns;

    private Properties properties = new Properties();

    private TimeoutConfiguration timeoutConfiguration;

    // default timeout is 2 seconds
    public static final long DEFAULT_TIMEOUT_MS = 2000;

    private ConsistencyChecker consistencyChecker;

    // used to associate inconsistencies with the service in the logs
    private final String implementationName;

    private ExceptionSwallowedListener exceptionSwallowedListener = null;

    /**
     * Facade swallows Lightblue implementation failures and returns from legacy implementation.
     */
    public static interface ExceptionSwallowedListener {
        public void onLightblueSvcExceptionSwallowed(Throwable t, String implementationName);
    }

    public SharedStore getSharedStore() {
        return sharedStore;
    }

    public void setSharedStore(SharedStore shareStore) {
        this.sharedStore = shareStore;

        legacySvc.setSharedStore(shareStore);
        lightblueSvc.setSharedStore(shareStore);
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

    public ServiceFacade(D legacySvc, D lightblueSvc, String implementationName) {
        this(legacySvc, lightblueSvc, implementationName, null);
    }

    public ServiceFacade(D legacySvc, D lightblueSvc, String implementationName, Properties properties) {
        super();
        this.legacySvc = legacySvc;
        this.lightblueSvc = lightblueSvc;
        setSharedStore(new SharedStoreImpl(implementationName));
        this.implementationName = implementationName;
        if (properties != null) {
            this.properties = properties;
        }

        timeoutConfiguration = new TimeoutConfiguration(DEFAULT_TIMEOUT_MS, implementationName, this.properties);

        log.info("Initialized facade for "+implementationName);
    }

    private long getLightblueExecutionTimeout(String methodName) {
        String timeout = properties.getProperty("com.redhat.lightblue.migrator.facade.timeout." + implementationName + "." + methodName);
        if (timeout == null) {
            timeout = properties.getProperty("com.redhat.lightblue.migrator.facade.timeout." + implementationName, "" + DEFAULT_TIMEOUT_MS);
        }

        return Long.parseLong(timeout);
    }

    private ListeningExecutorService createExecutor() {
        return MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    }

    private Class[] toClasses(Object[] objects) {
        List<Class> classes = new ArrayList<>();
        for (Object o : objects) {
            classes.add(o.getClass());
        }
        return classes.toArray(new Class[]{});
    }

    private <T> ListenableFuture<T> callLightblueSvc(final Method method, final Object[] values, final FacadeOperation op, final MethodCallStringifier callStringifier) {
        ListeningExecutorService executor = createExecutor();
        try {
        // fetch from lightblue using future (asynchronously)
        final long parentThreadId = Thread.currentThread().getId();
        return executor.submit(new Callable<T>(){
            @Override
            public T call() throws Exception {
                Timer dest = new Timer("destination."+method.getName());
                if (sharedStore != null) {
                    sharedStore.copyFromThread(parentThreadId);
                }
                try {
                    return (T) method.invoke(lightblueSvc, values);
                } finally {
                    long callTook = dest.complete();
                    long slowWarning = timeoutConfiguration.getSlowWarningMS(method.getName(), op);

                    if (callTook >= slowWarning) {
                        // call is slow; this will log even if source fails to respond
                        log.warn("Slow call warning: {}.{} took {}ms",implementationName, callStringifier.toString(), callTook);
                    }
                }
            }
        });
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Call destination (lightblue) using a timeout in dual read/write phases.
     * Do not use facade timeout during lightblue proxy and kinda proxy phases (when
     * reading from source is disabled).
     *
     * @param listenableFuture
     * @param methodName method name is used to read method specific timeout
     * configuration
     * @param destinationCallTimeout Set future timeout to this amount
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    private <T> T getWithTimeout(ListenableFuture<T> listenableFuture, String methodName, FacadeOperation facadeOperation, int destinationCallTimeout) throws InterruptedException, ExecutionException, TimeoutException {
        // not reading from source/legacy means this is either proxy or kinda proxy phase
        // in that case, ignore timeout settings for Lightblue call
        if (!shouldSource(FacadeOperation.READ) || timeoutConfiguration.getTimeoutMS(methodName, facadeOperation) <= 0) {
            return listenableFuture.get();
        } else {
            return listenableFuture.get(destinationCallTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private Throwable extractUnderlyingException(Throwable t) {
        if ((t instanceof ExecutionException || t instanceof InvocationTargetException) && t.getCause() != null) {
            return extractUnderlyingException(t.getCause());
        } else {
            return t;
        }
    }

    // which togglz flags to use
    public enum FacadeOperation {
        READ, WRITE;
    }

    private boolean shouldDestination(FacadeOperation facadeOperation) {
        switch (facadeOperation) {
            case READ:
                return LightblueMigration.shouldReadDestinationEntity();
            case WRITE:
                return LightblueMigration.shouldWriteDestinationEntity();
            default:
                throw new IllegalArgumentException(facadeOperation.toString());
        }
    }

    private boolean shouldSource(FacadeOperation facadeOperation) {
        switch (facadeOperation) {
            case READ:
                return LightblueMigration.shouldReadSourceEntity();
            case WRITE:
                return LightblueMigration.shouldWriteSourceEntity();
            default:
                throw new IllegalArgumentException(facadeOperation.toString());
        }
    }

    private boolean shouldCheckConsistency(FacadeOperation facadeOperation) {
        switch (facadeOperation) {
            case READ:
                return LightblueMigration.shouldCheckReadConsistency();
            case WRITE:
                return LightblueMigration.shouldCheckWriteConsistency();
            default:
                throw new IllegalArgumentException(facadeOperation.toString());
        }
    }

    private void cancel(ListenableFuture listenableFuture) {
        if (listenableFuture != null) {
            try {
                listenableFuture.cancel(true);
            } catch (Exception e) {
                log.error("Failed to cancel lightblue call", e);
            }
        }
    }

    /**
     * Call service method according to settings specified in togglz.
     *
     * @param facadeOperation READ/WRITE operation, used by togglz flags
     * @param callInParallel true means both oracle and lightblue will be called
     * in parallel and state sharing between services will not be possible
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param types List of parameter types
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callSvcMethod(final FacadeOperation facadeOperation, final boolean callInParallel, final Method methodCalled, final Object... values) throws Throwable {

        final Class<T> returnedType = (Class<T>) methodCalled.getReturnType();
        final String methodName = methodCalled.getName();
        final Class[] types = methodCalled.getParameterTypes();

        LazyMethodCallStringifier callStringifier = new LazyMethodCallStringifier(methodCalled, values);

        if (log.isDebugEnabled()) {
            log.debug("Calling {}.{} ({} {})", implementationName, callStringifier, callInParallel ? "parallel" : "serial", facadeOperation);
        }

        TogglzRandomUsername.init();

        if (sharedStore != null) {
            sharedStore.clear(); // make sure no data is left from previous calls
        }
        if (sharedStore != null && shouldSource(facadeOperation) && shouldDestination(facadeOperation)) {
            sharedStore.setDualMigrationPhase(true);
        } else if (sharedStore != null) {
            sharedStore.setDualMigrationPhase(false);
        }

        T legacyEntity = null, lightblueEntity = null;

        ListenableFuture<T> listenableFuture = null;

        if (callInParallel) {
            if (shouldDestination(facadeOperation)) {
                Method method = lightblueSvc.getClass().getMethod(methodName, types);
                listenableFuture = callLightblueSvc(method, values, facadeOperation, callStringifier);
            }
        }

        int legacyCallTookMS = 0;
        if (shouldSource(facadeOperation)) {
            // perform operation in oracle, synchronously
            log.debug("Calling legacy {}.{}", implementationName, methodName);
            Method method = legacySvc.getClass().getMethod(methodName, types);
            Timer source = new Timer("source." + methodName);
            try {
                legacyEntity = (T) method.invoke(legacySvc, values);
            } catch (InvocationTargetException e) {
                if (shouldDestination(facadeOperation) && !shouldCheckConsistency(facadeOperation)) {
                    // Lightblue is going to be called and consistency checker is disabled
                    // swallow the exception from legacy
                    log.warn("Legacy call " + implementationName + "." + callStringifier + " threw an exception. Returning data from Lightblue.", e.getCause());
                } else {
                    if (timeoutConfiguration.isInterruptOnTimeout() && facadeOperation == FacadeOperation.READ) {
                        cancel(listenableFuture);
                    }
                    throw e.getCause();
                }
            } finally {
                legacyCallTookMS = (int) source.complete();
            }
        }

        if (shouldDestination(facadeOperation)) {
            log.debug("Calling lightblue {}.{}", implementationName, methodName);

            int destinationCallTimeout = (int) Math.max(timeoutConfiguration.getTimeoutMS(methodName, facadeOperation), legacyCallTookMS);

            try {
                if (callInParallel) {
                    // lightblue was called before source/legacy, now just getting results
                    lightblueEntity = getWithTimeout(listenableFuture, methodName, facadeOperation, destinationCallTimeout);
                } else {
                    // call lightblue after source/legacy finished
                    Method method = lightblueSvc.getClass().getMethod(methodName, types);
                    listenableFuture = callLightblueSvc(method, values, facadeOperation, callStringifier);
                    lightblueEntity = getWithTimeout(listenableFuture, methodName, facadeOperation, destinationCallTimeout);
                }
            } catch (TimeoutException te) {

                if (timeoutConfiguration.isInterruptOnTimeout() && facadeOperation == FacadeOperation.READ) {
                    // try to interrupt the thread
                    cancel(listenableFuture);
                }

                if (shouldSource(facadeOperation)) {
                    log.warn("Lightblue call {}.{} is taking too long (longer than {}ms). Returning data from legacy.", implementationName, callStringifier, destinationCallTimeout);
                    return legacyEntity;
                } else {
                    throw te;
                }
            } catch (Throwable e) {
                if (shouldSource(facadeOperation) && shouldCheckConsistency(facadeOperation)) {
                    // swallow lightblue exception if legacy was called and consistency checker is on
                    log.warn("Lightblue call " + implementationName + "." + callStringifier + " threw an exception. Returning data from legacy.", e);
                    onExceptionSwallowed(e);
                    return legacyEntity;
                } else {
                    // throw lightblue exception if legacy was not called or consistency checker is disabled
                    throw extractUnderlyingException(e);
                }
            }
        }

        if (shouldCheckConsistency(facadeOperation) && shouldSource(facadeOperation) && shouldDestination(facadeOperation)) {
            // dual phase, consistency check enabled
            // make sure that response from lightblue and oracle are the same
            if (log.isDebugEnabled())
                log.debug("." + methodName + " checking returned entity's consistency");

            // check if entities match
            if (getConsistencyChecker().checkConsistency(legacyEntity, lightblueEntity, methodName, callStringifier)) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                return legacyEntity;
            }
        } else if (!shouldCheckConsistency(facadeOperation) && shouldSource(facadeOperation) && shouldDestination(facadeOperation)) {
            if (log.isDebugEnabled())
                log.debug("dual phase, no consistency check (disabled), returning data from Lightblue");
            return lightblueEntity;
        } else if (!shouldSource(facadeOperation) && shouldDestination(facadeOperation)) {
            if (log.isDebugEnabled())
                log.debug("proxy phase, returning data from Lightblue");
            return lightblueEntity;
        } else {
            if (log.isDebugEnabled())
                log.debug("initial phase, returning data from legacy");
            return legacyEntity;
        }
    }

    private SharedStoreException extractSharedStoreExceptionIfExists(ExecutionException ee) {
        try {
            if (ee.getCause().getCause() instanceof SharedStoreException) {
                return (SharedStoreException) ee.getCause().getCause();
            } else {
                return null;
            }
        } catch (NullPointerException e) {
            return null;
        }
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

    public D getLightblueSvc() {
        return lightblueSvc;
    }

    public TimeoutConfiguration getTimeoutConfiguration() {
        return timeoutConfiguration;
    }

    public void setTimeoutConfiguration(TimeoutConfiguration timeoutConfiguration) {
        this.timeoutConfiguration = timeoutConfiguration;
    }

    public ExceptionSwallowedListener getExceptionSwallowedListener() {
        return exceptionSwallowedListener;
    }

    public void registerExceptionSwallowedListener(ExceptionSwallowedListener exceptionSwallowedEvent) {
        this.exceptionSwallowedListener = exceptionSwallowedEvent;
    }

    private void onExceptionSwallowed(Throwable t) {
        if (exceptionSwallowedListener != null)
            exceptionSwallowedListener.onLightblueSvcExceptionSwallowed(t, implementationName);
    }
}
