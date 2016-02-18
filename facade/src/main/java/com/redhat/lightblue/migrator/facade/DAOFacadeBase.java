package com.redhat.lightblue.migrator.facade;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.redhat.lightblue.migrator.facade.ServiceFacade.FacadeOperation;
import com.redhat.lightblue.migrator.facade.methodcallstringifier.EagerMethodCallStringifier;
import com.redhat.lightblue.migrator.features.LightblueMigration;
import com.redhat.lightblue.migrator.features.TogglzRandomUsername;

/**
 * Deprecated. Does not support hiding secrets in the logs and slow call warnings. Use {@link ServiceFacade} instead.
 *
 * A helper base class for migrating services from legacy datastore to lightblue. It lets you call any service/dao method, using togglz switches to choose which
 * service/dao to use and verifying returned data. Verification uses equals method - use {@link BeanConsistencyChecker} in your beans for sophisticated
 * consistency check.
 *
 *
 * @author mpatercz
 *
 */
@SuppressWarnings("all")
@Deprecated
public class DAOFacadeBase<D> {

    private static final Logger log = LoggerFactory.getLogger(DAOFacadeBase.class);

    protected final D legacyDAO, lightblueDAO;

    private EntityIdStore entityIdStore = null;

    private Map<Class<?>,ModelMixIn> modelMixIns;

    private ConsistencyChecker consistencyChecker;

    // used to associate inconsistencies with the service in the logs
    private final String implementationName;

    private Properties properties = new Properties();

    private TimeoutConfiguration timeoutConfiguration;

    public EntityIdStore getEntityIdStore() {
        return entityIdStore;
    }

    public void setEntityIdStore(EntityIdStore entityIdStore) {
        this.entityIdStore = entityIdStore;

        try {
            Method method = lightblueDAO.getClass().getMethod("setEntityIdStore", EntityIdStore.class);
            method.invoke(lightblueDAO, entityIdStore);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("LightblueDAO needs to have a setter method for EntityIdStore", e);
        }
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

    public DAOFacadeBase(D legacyDAO, D lightblueDAO) {
        this(legacyDAO, lightblueDAO, null);
    }

    public DAOFacadeBase(D legacyDAO, D lightblueDAO, Properties properties) {
        super();
        this.legacyDAO = legacyDAO;
        this.lightblueDAO = lightblueDAO;
        setEntityIdStore(new EntityIdStoreImpl(this.getClass())); // this.getClass() will point at superclass
        this.implementationName = this.getClass().getSimpleName();

        if (properties != null)
            this.properties = properties;

        timeoutConfiguration = new TimeoutConfiguration(ServiceFacade.DEFAULT_TIMEOUT_MS, implementationName, properties);

        log.info("Initialized facade for "+implementationName);
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

    private <T> ListenableFuture<T> callLightblueDAO(final boolean passIds, final Method method, final Object[] values) {
        ListeningExecutorService executor = createExecutor();
        try {
        // fetch from lightblue using future (asynchronously)
        final long parentThreadId = Thread.currentThread().getId();
        return executor.submit(new Callable<T>(){
            @Override
            public T call() throws Exception {
                Timer dest = new Timer("destination."+method.getName());
                if (passIds)
                    entityIdStore.copyFromThread(parentThreadId);
                try {
                    return (T) method.invoke(lightblueDAO, values);
                } finally {
                    dest.complete();
                }
            }
        });
        } finally {
            executor.shutdown();
        }
    }

    private <T> T getWithTimeout(ListenableFuture<T> listenableFuture, String methodName, FacadeOperation op, boolean shouldSource) throws InterruptedException, ExecutionException, TimeoutException {
        if (!shouldSource || timeoutConfiguration.getTimeoutMS(methodName, op) <= 0) {
            return listenableFuture.get();
        } else {
            return listenableFuture.get(timeoutConfiguration.getTimeoutMS(methodName, op), TimeUnit.MILLISECONDS);
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

    /**
     * Call dao method which reads data.
     *
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param types List of parameter types
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOReadMethod(final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Throwable {
        if (log.isDebugEnabled())
            log.debug("Calling {}.{} ({} {})", implementationName, EagerMethodCallStringifier.stringifyMethodCall(methodName, values), "parallel", "READ");
        TogglzRandomUsername.init();

        T legacyEntity = null, lightblueEntity = null;
        ListenableFuture<T> listenableFuture = null;

        if (LightblueMigration.shouldReadDestinationEntity()) {
            Method method = lightblueDAO.getClass().getMethod(methodName, types);
            listenableFuture = callLightblueDAO(false, method, values);
        }

        if (LightblueMigration.shouldReadSourceEntity()) {
            // fetch from oracle, synchronously
            log.debug("Calling legacy {}.{}", implementationName, methodName);
            Method method = legacyDAO.getClass().getMethod(methodName,types);
            Timer source = new Timer("source."+methodName);
            try {
                legacyEntity = (T) method.invoke(legacyDAO, values);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } finally {
                source.complete();
            }
        }

        if (LightblueMigration.shouldReadDestinationEntity()) {
            // make sure async call to lightblue has completed
            try {
                log.debug("Calling lightblue {}.{}", implementationName, methodName);
                lightblueEntity = getWithTimeout(listenableFuture, methodName, FacadeOperation.READ, LightblueMigration.shouldReadSourceEntity());
            } catch (TimeoutException te) {
                if (LightblueMigration.shouldReadSourceEntity()) {
                    log.warn("Lightblue call "+implementationName+"."+EagerMethodCallStringifier.stringifyMethodCall(methodName, values)+" is taking too long (longer than "+timeoutConfiguration.getTimeoutMS(methodName, FacadeOperation.READ)+"s). Returning data from legacy.");
                    return legacyEntity;
                } else {
                    throw te;
                }
            } catch (Throwable e) {
                if (LightblueMigration.shouldReadSourceEntity()) {
                    log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                    return legacyEntity;
                } else {
                    throw extractUnderlyingException(e);
                }
            }
        }

        if (LightblueMigration.shouldCheckReadConsistency() && LightblueMigration.shouldReadSourceEntity() &&  LightblueMigration.shouldReadDestinationEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");
            if (getConsistencyChecker().checkConsistency(legacyEntity, lightblueEntity, methodName, new EagerMethodCallStringifier(methodName, values))) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                return legacyEntity;
            }
        }

        return lightblueEntity != null ? lightblueEntity : legacyEntity;
    }

    public <T> List<T> callDAOReadMethodReturnList(final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Throwable {
        return (List<T>)callDAOReadMethod(returnedType, methodName, types, values);
    }

    /**
     * Call dao method which reads data. Won't work if method has primitive parameters.
     *
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOReadMethod(final Class<T> returnedType, final String methodName, final Object ... values) throws Throwable {
        return callDAOReadMethod(returnedType, methodName, toClasses(values), values);
    }

    /**
     * Call dao method which updates data. Updating makes sense only for entities with known ID. If ID is not specified, it will be generated
     * by both legacy and lightblue datastores independently, creating a data inconsistency. If you don't know the ID, use on of the callDAOCreate methods.
     *
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param types List of parameter types
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOUpdateMethod(final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Throwable {
        if (log.isDebugEnabled())
            log.debug("Calling {}.{} ({} {})", implementationName, EagerMethodCallStringifier.stringifyMethodCall(methodName, values), "parallel", "WRITE");
        TogglzRandomUsername.init();

        T legacyEntity = null, lightblueEntity = null;
        ListenableFuture<T> listenableFuture = null;

        if (LightblueMigration.shouldWriteDestinationEntity()) {
            Method method = lightblueDAO.getClass().getMethod(methodName, types);
            listenableFuture = callLightblueDAO(false, method, values);
        }

        if (LightblueMigration.shouldWriteSourceEntity()) {
            // fetch from oracle, synchronously
            log.debug("Calling legacy {}.{}", implementationName, methodName);
            Method method = legacyDAO.getClass().getMethod(methodName,types);
            Timer source = new Timer("source."+methodName);
            try {
                legacyEntity = (T) method.invoke(legacyDAO, values);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } finally {
                source.complete();
            }
        }

        if (LightblueMigration.shouldWriteDestinationEntity()) {
            // make sure asnyc call to lightblue has completed
            log.debug("Calling lightblue {}.{}", implementationName, methodName);
            try {
                lightblueEntity = getWithTimeout(listenableFuture, methodName, FacadeOperation.WRITE, LightblueMigration.shouldWriteSourceEntity());
            } catch (TimeoutException te) {
                if (LightblueMigration.shouldReadSourceEntity()) {
                    log.warn("Lightblue call "+implementationName+"."+EagerMethodCallStringifier.stringifyMethodCall(methodName, values)+" is taking too long (longer than "+timeoutConfiguration.getTimeoutMS(methodName, FacadeOperation.WRITE)+"s). Returning data from legacy.");
                    return legacyEntity;
                } else {
                    throw te;
                }
            } catch (Throwable e) {
                if (LightblueMigration.shouldReadSourceEntity()) {
                    log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                    return legacyEntity;
                } else {
                    throw extractUnderlyingException(e);
                }
            }
        }

        if (LightblueMigration.shouldCheckWriteConsistency() && LightblueMigration.shouldWriteSourceEntity() && LightblueMigration.shouldWriteDestinationEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");
            if (getConsistencyChecker().checkConsistency(legacyEntity, lightblueEntity, methodName, new EagerMethodCallStringifier(methodName, values))) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                return legacyEntity;
            }
        }

        return lightblueEntity != null ? lightblueEntity : legacyEntity;
    }

    /**
     * Call dao method which updates data. Won't work if method has primitive parameters.
     *
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOUpdateMethod(final Class<T> returnedType, final String methodName, final Object ... values) throws Throwable {
        return callDAOUpdateMethod(returnedType, methodName, toClasses(values), values);
    }

    /**
     * Call dao method which creates a single entity. It will ensure that entities in both legacy and lightblue datastores are the same, including IDs.
     *
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param types List of parameter types
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOCreateSingleMethod(final EntityIdExtractor<T> entityIdExtractor, final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Throwable {
        if (log.isDebugEnabled())
            log.debug("Calling {}.{} ({} {})", implementationName, EagerMethodCallStringifier.stringifyMethodCall(methodName, values), "serial", "WRITE");
        TogglzRandomUsername.init();

        if (entityIdStore != null)
            entityIdStore.clear();

        T legacyEntity = null, lightblueEntity = null;

        if (LightblueMigration.shouldWriteSourceEntity()) {
            // insert to oracle, synchronously
            log.debug("Calling legacy {}.{}", implementationName, methodName);
            Method method = legacyDAO.getClass().getMethod(methodName,types);
            Timer source = new Timer("source."+methodName);
            try {
                legacyEntity = (T) method.invoke(legacyDAO, values);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } finally {
                source.complete();
            }
        }

        if (LightblueMigration.shouldWriteDestinationEntity()) {
            log.debug("Calling lightblue {}.{}", implementationName, methodName);

            // don't attempt to pass ids when entity returned from legacy is null
            boolean passIds = entityIdStore != null && legacyEntity != null;

            try {
                if (passIds) {
                    Long id = entityIdExtractor.extractId(legacyEntity);
                    entityIdStore.push(id);
                }
            } catch (Exception e) {
                log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                return legacyEntity;
            }

            // it's expected that this method in lightblueDAO will extract id from idStore
            Method method = lightblueDAO.getClass().getMethod(methodName, types);
            ListenableFuture<T> listenableFuture = callLightblueDAO(passIds, method, values);

            try {
                lightblueEntity = getWithTimeout(listenableFuture, methodName, FacadeOperation.WRITE, LightblueMigration.shouldWriteSourceEntity());
            } catch (ExecutionException ee) {
                if (LightblueMigration.shouldReadSourceEntity()) {
                    EntityIdStoreException se = extractEntityIdStoreExceptionIfExists(ee);
                    if (se != null && !passIds) {
                        log.warn("Possible data inconsistency in a create-if-not-exists scenario (entity exists in legacy, but does not in lightblue). Method called: "
                                + EagerMethodCallStringifier.stringifyMethodCall(methodName, values), se);
                        return legacyEntity;
                    }

                    log.warn("Error when calling lightblue DAO. Returning data from legacy.", ee);
                    return legacyEntity;
                } else {
                    throw extractUnderlyingException(ee);
                }
            } catch (TimeoutException te) {
                if (LightblueMigration.shouldReadSourceEntity()) {
                    log.warn("Lightblue call "+implementationName+"."+EagerMethodCallStringifier.stringifyMethodCall(methodName, values)+" is taking too long (longer than "+timeoutConfiguration.getTimeoutMS(methodName, FacadeOperation.WRITE)+"s). Returning data from legacy.");
                    return legacyEntity;
                } else {
                    throw te;
                }
            } catch (Throwable e) {
                if (LightblueMigration.shouldReadSourceEntity()) {
                    log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                    return legacyEntity;
                } else {
                    throw extractUnderlyingException(e);
                }
            }

        }

        if (LightblueMigration.shouldCheckWriteConsistency() && LightblueMigration.shouldWriteSourceEntity() && LightblueMigration.shouldWriteDestinationEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");

            // check if entities match
            if (getConsistencyChecker().checkConsistency(legacyEntity, lightblueEntity, methodName, new EagerMethodCallStringifier(methodName, values))) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                return legacyEntity;
            }
        }

        return lightblueEntity != null ? lightblueEntity : legacyEntity;
    }

    private EntityIdStoreException extractEntityIdStoreExceptionIfExists(ExecutionException ee) {
        try {
            if (ee.getCause().getCause() instanceof EntityIdStoreException) {
                return (EntityIdStoreException)ee.getCause().getCause();
            } else {
                return null;
            }
        } catch (NullPointerException e) {
            return null;
        }
    }

    public <T> T callDAOCreateSingleMethod(final EntityIdExtractor<T> entityIdExtractor, final Class<T> returnedType, final String methodName, final Object ... values) throws Throwable {
        return callDAOCreateSingleMethod(entityIdExtractor, returnedType, methodName, toClasses(values), values);
    }

    @Deprecated
    public int getTimeoutSeconds() {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutConfiguration = new TimeoutConfiguration(timeoutSeconds*1000, implementationName, properties);
    }

    public void setLogResponseDataEnabled(boolean logResponsesEnabled) {
        getConsistencyChecker().setLogResponseDataEnabled(logResponsesEnabled);
    }

    public void setMaxInconsistencyLogLength(int length) {
        getConsistencyChecker().setMaxInconsistencyLogLength(length);
    }

    public D getLegacyDAO() {
        return legacyDAO;
    }

    public TimeoutConfiguration getTimeoutConfiguration() {
        return timeoutConfiguration;
    }

    public void setTimeoutConfiguration(TimeoutConfiguration timeoutConfiguration) {
        this.timeoutConfiguration = timeoutConfiguration;
    }
}
