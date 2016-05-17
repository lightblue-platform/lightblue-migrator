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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
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
 * A helper base class for migrating services from legacy datastore to lightblue. It lets you call any service/dao method, using togglz switches to choose which
 * service/dao to use and verifying returned data.
 *
 * @author mpatercz
 *
 */
@SuppressWarnings("all")
public class ServiceFacade<D extends SharedStoreSetter> implements SharedStoreSetter {

    private static final Logger log = LoggerFactory.getLogger(ServiceFacade.class);

    protected final D legacySvc, lightblueSvc;

    private SharedStore sharedStore = null;

    private Map<Class<?>,ModelMixIn> modelMixIns;

    // facade properties
    private Properties properties = new Properties();

    // facade properties prefix
    public static final String CONFIG_PREFIX = "com.redhat.lightblue.migrator.facade.";

    private TimeoutConfiguration timeoutConfiguration;

    // default timeout is 2 seconds
    public static final long DEFAULT_TIMEOUT_MS = 2000;

    private ConsistencyChecker consistencyChecker;

    // used to associate inconsistencies with the service in the logs
    private final String implementationName;

    // thread pool singleton
    // it is safe to submit to a single executor from multiple threads
    // (http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html)
    private static volatile ListeningExecutorService executor = null;

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
        if (properties != null)
            this.properties = properties;

        timeoutConfiguration = new TimeoutConfiguration(DEFAULT_TIMEOUT_MS, implementationName, this.properties);

        // create ThreadPoolExecutor for all facades
        if (executor == null) {
            synchronized("executor") {
                if (executor == null) {
                    int threadPoolSize = Integer.parseInt(this.properties.getProperty(CONFIG_PREFIX+implementationName+".threadPool.size", "50"));

                    // cached means automatic thread reclamation
                    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
                    // fixed size, no queuing
                    threadPoolExecutor.setCorePoolSize(threadPoolSize);
                    threadPoolExecutor.setMaximumPoolSize(threadPoolSize);
                    executor = MoreExecutors.listeningDecorator(threadPoolExecutor);
                    log.info("Initialized facade threadPool.size={} for {}."+
                            "Note it may be used by other services (there is a single, shared thread pool for all facaded services in the deployment",
                            threadPoolSize, implementationName);
                }
            }
        }

        log.info("Initialized facade for "+implementationName);
    }

    private long getLightblueExecutionTimeout(String methodName) {
        String timeout = properties.getProperty("com.redhat.lightblue.migrator.facade.timeout."+implementationName+"."+methodName);
        if (timeout == null)
            timeout = properties.getProperty("com.redhat.lightblue.migrator.facade.timeout."+implementationName, ""+DEFAULT_TIMEOUT_MS);

        return Long.parseLong(timeout);
    }

    private Class[] toClasses(Object[] objects) {
        List<Class> classes = new ArrayList<>();
        for (Object o: objects) {
            classes.add(o.getClass());
        }
        return classes.toArray(new Class[]{});
    }

    private <T> ListenableFuture<T> callLightblueSvc(final Method method, final Object[] values, final FacadeOperation op, final MethodCallStringifier callStringifier) {
        try {
            // fetch from lightblue using future (asynchronously)
            final long parentThreadId = Thread.currentThread().getId();
            return executor.submit(new Callable<T>(){
                @Override
                public T call() throws Exception {
                    Timer dest = new Timer("destination."+method.getName());
                    if (sharedStore != null)
                        sharedStore.copyFromThread(parentThreadId);
                    try {
                        return (T) method.invoke(lightblueSvc, values);
                    } finally {
                        long callTook = dest.complete();
                        long timeout = timeoutConfiguration.getTimeoutMS(method.getName(), op);
                        long slowWarning = timeoutConfiguration.getSlowWarningMS(method.getName(), op);

                        if (callTook >= slowWarning && callTook < timeout) {
                            // call is slow but not slow enough to trigger timeout
                            log.warn("Slow call warning: {}.{} took {}ms",implementationName, callStringifier.toString(), callTook);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            // this may indicate that lightblue has a problem and threads are piling up on the client side
            // max thread pool size is there to protect the client
            log.error("Lightblue call {}.{} was not executed because thread pool is full.", implementationName, callStringifier);
            if (!shouldSource(op)) {
                // there is no fallback, throw the exception
                throw e;
            }
            return null;
        }
    }

    /**
     * Call destination (lightblue) using a timeout in dual read/write phases. Do not use facade
     * timeout during lightblue proxy phase.
     *
     * @param listenableFuture
     * @param methodName method name is used to read method specific timeout configuration
     * @param shouldSource true if source operation is supposed to be performed for this call (i.e. this is a dual phase)
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    private <T> T getWithTimeout(ListenableFuture<T> listenableFuture, String methodName, FacadeOperation facadeOperation, boolean shouldSource) throws InterruptedException, ExecutionException, TimeoutException {
        if (!shouldSource || timeoutConfiguration.getTimeoutMS(methodName, facadeOperation) <= 0) {
            return listenableFuture.get();
        } else {
            return listenableFuture.get(timeoutConfiguration.getTimeoutMS(methodName, facadeOperation), TimeUnit.MILLISECONDS);
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
    public <T> T callSvcMethod(final FacadeOperation facadeOperation, final boolean callInParallel, final Method methodCalled, final Object ... values) throws Throwable {

        final Class<T> returnedType = (Class<T>) methodCalled.getReturnType();
        final String methodName = methodCalled.getName();
        final Class[] types = methodCalled.getParameterTypes();

        LazyMethodCallStringifier callStringifier = new LazyMethodCallStringifier(methodCalled, values);

        if (log.isDebugEnabled()) {
            log.debug("Calling {}.{} ({} {})", implementationName, callStringifier, callInParallel ? "parallel": "serial", facadeOperation);
        }

        TogglzRandomUsername.init();

        if (sharedStore != null)
            sharedStore.clear(); // make sure no data is left from previous calls

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
                listenableFuture = callLightblueSvc(method, values, facadeOperation, callStringifier);
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
                    if (listenableFuture == null) {
                        log.debug("Lightblue was not called because thread pool is full");
                        return legacyEntity;
                    }
                    lightblueEntity = getWithTimeout(listenableFuture, methodName, facadeOperation, shouldSource(facadeOperation));
                } else {
                    // call lightblue after source/legacy finished
                    Method method = lightblueSvc.getClass().getMethod(methodName, types);
                    listenableFuture = callLightblueSvc(method, values, facadeOperation, callStringifier);
                    if (listenableFuture == null) {
                        log.debug("Lightblue was not called because thread pool is full");
                        return legacyEntity;
                    }
                    lightblueEntity = getWithTimeout(listenableFuture, methodName, facadeOperation, shouldSource(facadeOperation));
                }
            } catch (TimeoutException te) {
                if (shouldSource(facadeOperation)) {
                    log.warn("Lightblue call "+implementationName+"."+callStringifier+" is taking too long (longer than "+timeoutConfiguration.getTimeoutMS(methodName, facadeOperation)+"s). Returning data from legacy.");
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
            if (getConsistencyChecker().checkConsistency(legacyEntity, lightblueEntity, methodName, callStringifier)) {
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

    public D getLightblueSvc() {
        return lightblueSvc;
    }

    public TimeoutConfiguration getTimeoutConfiguration() {
        return timeoutConfiguration;
    }

    public void setTimeoutConfiguration(TimeoutConfiguration timeoutConfiguration) {
        this.timeoutConfiguration = timeoutConfiguration;
    }

    /**
     * Shutdown thread pool executor for all facades.
     *
     */
    public static void shutdown() {
        if (executor != null) {
            synchronized ("executor") {
                if (executor != null) {
                    executor.shutdown();
                    executor = null;
                }
            }
        }
    }
}
