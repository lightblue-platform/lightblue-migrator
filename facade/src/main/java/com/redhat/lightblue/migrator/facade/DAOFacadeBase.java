package com.redhat.lightblue.migrator.facade;

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

import org.reflections.Reflections;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.redhat.lightblue.migrator.features.LightblueMigration;
import com.redhat.lightblue.migrator.features.TogglzRandomUsername;

/**
 * A helper base class for migrating services from legacy datastore to lightblue. It lets you call any service/dao method, using togglz switches to choose which
 * service/dao to use and verifying returned data. Verification uses equals method - use {@link BeanConsistencyChecker} in your beans for sophisticated
 * consistency check.
 *
 * @author mpatercz
 *
 */
@SuppressWarnings("all")
public class DAOFacadeBase<D> {

    private static final Logger log = LoggerFactory.getLogger(DAOFacadeBase.class);

    protected final D legacyDAO, lightblueDAO;

    private EntityIdStore entityIdStore = null;

    private Map<Class<?>,ModelMixIn> modelMixIns;

    private int timeoutSeconds = 0;

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

    public DAOFacadeBase(D legacyDAO, D lightblueDAO) {
        super();
        this.legacyDAO = legacyDAO;
        this.lightblueDAO = lightblueDAO;
        setEntityIdStore(new EntityIdStoreImpl(this.getClass())); // this.getClass() will point at superclass
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
        return mapper.writer().withDefaultPrettyPrinter();
    }

    public boolean checkConsistency(Object o1, Object o2) {
        return checkConsistency(o1,o2,null);
    }

    public boolean checkConsistency(Object o1, Object o2, String methodName) {
        if (o1==null&o2==null) {
            return true;
        }
        try {
            long t1 = System.currentTimeMillis();
            String legacyJson = getObjectWriter(methodName).writeValueAsString(o1);
            String lightblueJson = getObjectWriter(methodName).writeValueAsString(o2);

            JSONCompareResult result = JSONCompare.compareJSON(legacyJson, lightblueJson, JSONCompareMode.LENIENT);
            long t2 = System.currentTimeMillis();

            log.debug("Consistency check took: " + (t2-t1)+" ms");
            log.info("Consistency Check Passed: "+ result.passed());
            if (log.isDebugEnabled() && !result.passed()) {
                log.debug("Consistency Results:\n"+ result.getMessage());
            }
            return result.passed();
        } catch (Exception e) {
            log.error("Consistency check failed! Invalid JSON. " + e.getMessage());
        }
        return false;
    }

    private ListeningExecutorService createExecutor() {
        return MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    }

    private Class[] toClasses(Object[] objects) {
        List<Class> classes = new ArrayList<Class>();
        for (Object o: objects) {
            classes.add(o.getClass());
        }
        return classes.toArray(new Class[]{});
    }

    private String methodCallToString(String methodName, Object[] values) {
        StringBuffer str = new StringBuffer();
        str.append(methodName+"(");
        Iterator<Object> it = Arrays.asList(values).iterator();
        while(it.hasNext()) {
            Object value = it.next();
            str.append(value);
            if (it.hasNext()) {
                str.append(", ");
            }
        }
        str.append(")");
        return str.toString();
    }

    private void logInconsistency(String entityName, String methodName, Object[] values, Object legacyEntity, Object lightblueEntity) {
        log.warn(entityName+" inconsistency in "+methodCallToString(methodName, values)+". Lightblue entity="+lightblueEntity+", returning legacy entity="+legacyEntity);
    }

    private <T> ListenableFuture<T> callLightblueDAO(final boolean passIds, final Method method, final Object[] values) {
        ListeningExecutorService executor = createExecutor();
        try {
        // fetch from lightblue using future (asynchronously)
        log.debug("."+method.getName()+" writing to lightblue");
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

    private <T> T getWithTimeout(ListenableFuture<T> listenableFuture) throws InterruptedException, ExecutionException, TimeoutException {
        if (timeoutSeconds <= 0) {
            return listenableFuture.get();
        } else {
            return listenableFuture.get(timeoutSeconds, TimeUnit.SECONDS);
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
        log.debug("Reading "+returnedType.getName()+" "+methodCallToString(methodName, values));
        TogglzRandomUsername.init();

        T legacyEntity = null, lightblueEntity = null;
        ListenableFuture<T> listenableFuture = null;

        if (LightblueMigration.shouldReadDestinationEntity()) {
            Method method = lightblueDAO.getClass().getMethod(methodName, types);
            listenableFuture = callLightblueDAO(false, method, values);
        }

        if (LightblueMigration.shouldReadSourceEntity()) {
            // fetch from oracle, synchronously
            log.debug("."+methodName+" reading from legacy");
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
            // make sure asnyc call to lightblue has completed
            try {
                lightblueEntity = getWithTimeout(listenableFuture);
            } catch (TimeoutException te) {
                log.warn("Lightblue call is taking too long (longer than "+timeoutSeconds+"s). Returning data from legacy.", te);
                return legacyEntity;
            } catch (Exception e) {
                log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                return legacyEntity;
            }
        }

        if (LightblueMigration.shouldCheckReadConsistency() && LightblueMigration.shouldReadSourceEntity() &&  LightblueMigration.shouldReadDestinationEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");
            if (checkConsistency(legacyEntity, lightblueEntity, methodName)) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                logInconsistency(returnedType.getName(), methodName, values, legacyEntity, lightblueEntity);
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
        log.debug("Writing "+(returnedType!=null?returnedType.getName():"")+" "+methodCallToString(methodName, values));
        TogglzRandomUsername.init();

        T legacyEntity = null, lightblueEntity = null;
        ListenableFuture<T> listenableFuture = null;

        if (LightblueMigration.shouldWriteDestinationEntity()) {
            Method method = lightblueDAO.getClass().getMethod(methodName, types);
            listenableFuture = callLightblueDAO(false, method, values);
        }

        if (LightblueMigration.shouldWriteSourceEntity()) {
            // fetch from oracle, synchronously
            log.debug("."+methodName+" writing to legacy");
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
            try {
                lightblueEntity = getWithTimeout(listenableFuture);
            } catch (TimeoutException te) {
                log.warn("Lightblue call is taking too long (longer than "+timeoutSeconds+"s). Returning data from legacy.", te);
                return legacyEntity;
            } catch (Exception e) {
                log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                return legacyEntity;
            }
        }

        if (LightblueMigration.shouldCheckWriteConsistency() && LightblueMigration.shouldWriteSourceEntity() && LightblueMigration.shouldWriteDestinationEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");
            if (checkConsistency(legacyEntity, lightblueEntity, methodName)) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                logInconsistency(returnedType.getName(), methodName, values, legacyEntity, lightblueEntity);
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
     * @param pureWrite method does only write if true. Set it to false if does both read and write.
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param types List of parameter types
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOCreateSingleMethod(final boolean pureWrite, final EntityIdExtractor<T> entityIdExtractor, final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Throwable {
        log.debug("Creating "+(returnedType!=null?returnedType.getName():"")+" "+methodCallToString(methodName, values));
        TogglzRandomUsername.init();

        T legacyEntity = null, lightblueEntity = null;

        if (LightblueMigration.shouldWriteSourceEntity()) {
            // insert to oracle, synchronously
            log.debug("."+methodName+" creating in legacy");
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

        if (!pureWrite && LightblueMigration.shouldWriteSourceEntity() && !LightblueMigration.shouldReadDestinationEntity()) {
            // dual write phase - use legacy dao, because does also a read
            log.debug("Dual write phase, skipping lightblueDAO for ."+methodName+" because that method does also a read");
            return legacyEntity;
        }

        if (LightblueMigration.shouldWriteDestinationEntity()) {
            log.debug("."+methodName+" creating in lightblue");

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
                lightblueEntity = getWithTimeout(listenableFuture);
            } catch (ExecutionException ee) {
                EntityIdStoreException se = extractEntityIdStoreExceptionIfExists(ee);
                if (se != null && !passIds) {
                    log.warn("Possible data inconsistency in a create-if-not-exists scenario (entity exists in legacy, but does not in lightblue). Method called: "
                            + methodCallToString(methodName, values), se);
                    return legacyEntity;
                }

                log.warn("Error when calling lightblue DAO. Returning data from legacy.", ee);
                return legacyEntity;
            } catch (TimeoutException te) {
                log.warn("Lightblue call is taking too long (longer than "+timeoutSeconds+"s). Returning data from legacy.", te);
                return legacyEntity;
            } catch (Exception e) {
                log.warn("Error when calling lightblue DAO. Returning data from legacy.", e);
                return legacyEntity;
            }

        }

        if (LightblueMigration.shouldCheckWriteConsistency() && LightblueMigration.shouldWriteSourceEntity() && LightblueMigration.shouldWriteDestinationEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");

            // check if entities match
            if (checkConsistency(lightblueEntity, legacyEntity, methodName)) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                logInconsistency(returnedType.getName(), methodName, values, legacyEntity, lightblueEntity);
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

    public <T> T callDAOCreateSingleMethod(final boolean pureWrite, final EntityIdExtractor<T> entityIdExtractor, final Class<T> returnedType, final String methodName, final Object ... values) throws Throwable {
        return callDAOCreateSingleMethod(pureWrite, entityIdExtractor, returnedType, methodName, toClasses(values), values);
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

}
