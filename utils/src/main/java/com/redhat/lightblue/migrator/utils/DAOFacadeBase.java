package com.redhat.lightblue.migrator.utils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.redhat.lightblue.migrator.features.LightblueMigration;
import com.redhat.lightblue.migrator.features.LightblueMigrationConfiguration;

/**
 * A helper base class for migrating services from legacy datastore to lightblue. It lets you call any service/dao method, using togglz switches to choose which service/dao to use and verifying
 * returned data. Verification uses equals method.
 *
 * @author mpatercz
 *
 */
@SuppressWarnings("all")
public class DAOFacadeBase {

    private static final Logger log = LoggerFactory.getLogger(LightblueMigrationConfiguration.class);

    protected final Object legacyDAO, lightblueDAO;

    public DAOFacadeBase(Object legacyDAO, Object lightblueDAO) {
        super();
        this.legacyDAO = legacyDAO;
        this.lightblueDAO = lightblueDAO;
    }

    private ListeningExecutorService getExecutor() {
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
            str.append(value.toString());
            if (it.hasNext()) {
                str.append(", ");
            }
        }
        str.append(")");
        return str.toString();
    }

    private void logInconsistency(String entityName, String methodName, Object[] values) {
        log.error(entityName+" inconsistency in "+methodCallToString(methodName, values));
    }

    /**
     * Call dao method which reads data
     *
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param types List of parameter types
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOReadMethod(final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Exception {
        log.debug("Reading "+returnedType.getName()+" "+methodCallToString(methodName, values));

        T legacyEntity = null, lightblueEntity = null;
        ListenableFuture<T> listenableFuture = null;

        if (LightblueMigration.shouldReadDestinationEntity()) {
            // fetch from lightblue using future (asynchronously)
            log.debug("."+methodName+" reading from lightblue");
            listenableFuture = getExecutor().submit(new Callable<T>(){
                @Override
                public T call() throws Exception {
                    Method method = lightblueDAO.getClass().getMethod(methodName, types);
                    return (T) method.invoke(lightblueDAO, values);
                }
            });
        }

        if (LightblueMigration.shouldReadSourceEntity()) {
            // fetch from oracle, synchronously
            log.debug("."+methodName+" reading from legacy");
            Method method = legacyDAO.getClass().getMethod(methodName,types);
            legacyEntity = (T) method.invoke(legacyDAO, values);
        }

        if (LightblueMigration.shouldReadDestinationEntity()) {
            // make sure asnyc call to lightblue has completed
            lightblueEntity = listenableFuture.get();
        }

        if (LightblueMigration.shouldCheckReadConsistency() && LightblueMigration.shouldReadSourceEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");
            Method equalsMethod = lightblueEntity.getClass().getMethod("equals", Object.class);
            if ((Boolean) equalsMethod.invoke(lightblueEntity, legacyEntity)) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                logInconsistency(returnedType.getName(), methodName, values);
                return legacyEntity;
            }
        }

        return lightblueEntity != null ? lightblueEntity : legacyEntity;
    }

    /**
     * Call dao method. Won't work if method has primitive parameters.
     *
     * @param returnedType type of the returned object
     * @param methodName method name to call
     * @param values List of parameters
     * @return Object returned by dao
     * @throws Exception
     */
    public <T> T callDAOReadMethod(final Class<T> returnedType, final String methodName, final Object ... values) throws Exception {
        return callDAOReadMethod(returnedType, methodName, toClasses(values), values);
    }

    public <T> T callDAOWriteMethod(final Class<T> returnedType, final String methodName, final Class[] types, final Object ... values) throws Exception {
        log.debug("Writing "+(returnedType!=null?returnedType.getName():"")+" "+methodCallToString(methodName, values));

        T legacyEntity = null, lightblueEntity = null;
        ListenableFuture<T> listenableFuture = null;

        if (LightblueMigration.shouldWriteDestinationEntity()) {
            // fetch from lightblue using future (asynchronously)
            log.debug("."+methodName+" writing to lightblue");
            listenableFuture = getExecutor().submit(new Callable<T>(){
                @Override
                public T call() throws Exception {
                    Method method = lightblueDAO.getClass().getMethod(methodName, types);
                    return (T) method.invoke(lightblueDAO, values);
                }
            });
        }

        if (LightblueMigration.shouldWriteSourceEntity()) {
            // fetch from oracle, synchronously
            log.debug("."+methodName+" writing to legacy");
            Method method = legacyDAO.getClass().getMethod(methodName,types);
            legacyEntity = (T) method.invoke(legacyDAO, values);
        }

        if (LightblueMigration.shouldWriteDestinationEntity()) {
            // make sure asnyc call to lightblue has completed
            lightblueEntity = listenableFuture.get();
        }

        if (LightblueMigration.shouldCheckWriteConsistency() && LightblueMigration.shouldWriteSourceEntity()) {
            // make sure that response from lightblue and oracle are the same
            log.debug("."+methodName+" checking returned entity's consistency");

            if (lightblueEntity == null && legacyEntity == null) {
                return null;
            }

            if (lightblueEntity == null && legacyEntity != null) {
                logInconsistency(returnedType.getName(), methodName, values);
                return legacyEntity;
            }

            Method equalsMethod = lightblueEntity.getClass().getMethod("equals", Object.class);
            if ((Boolean) equalsMethod.invoke(lightblueEntity, legacyEntity)) {
                // return lightblue data if they are
                return lightblueEntity;
            } else {
                // return oracle data if they aren't and log data inconsistency
                logInconsistency(returnedType.getName(), methodName, values);
                return legacyEntity;
            }
        }

        return lightblueEntity != null ? lightblueEntity : legacyEntity;
    }

}
