package com.redhat.lightblue.migrator.consistency;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractCollection;
import java.util.Date;
import java.util.Objects;

import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks beans for consistency, field by field. Using the checker inside equals method will provide
 * support for containers and arrays. Behavior:
 * <ol>
 * <li>If there is a type mismatch, beans are not consistent.</li>
 * <li>If both beans are null, beans are consistent.</li>
 * <li>Fields are compared using <code>Objects.equals(f1, f2)</code>.</li>
 * <li>Containers and arrays are compared using <code>Objects.equals(f1, f2)</code>.</li>
 * <li>Use {@link ConsistencyCheck} annotation to ignore certain fields when doing the consistency check.</li>
 * </ol>
 *
 * @author mpatercz
 *
 */
public class BeanConsistencyChecker {

    private static final Logger logger = LoggerFactory.getLogger(BeanConsistencyChecker.class);

    private boolean logInconsistenciesAsWarnings = true;

    public BeanConsistencyChecker() {}

    public BeanConsistencyChecker(boolean logWarnings) {
        super();
        this.logInconsistenciesAsWarnings = logWarnings;
    }

    public boolean consistent(final Object o1, final Object o2) {
        if (logger.isDebugEnabled())
            logger.debug("Checking object1="+o1+" against object2="+o2);

        if (o1 == null && o2 == null)
            return true;

        if (o1 == null && o2 != null)
            return false;

        for (Field field : o1.getClass().getDeclaredFields()) {
            if (consistencyCheckRequired(field)) {

                if (o2 == null) {
                    return false;
                }

                if (!o1.getClass().isInstance(o2) || !o2.getClass().isInstance(o1)) {
                    logger.debug("Types do not match");
                    return false;
                }

                if (o1 instanceof Object[] || o1 instanceof AbstractCollection) {
                    logger.debug("This is not a bean, it is a container. Performing standard equals check");
                    return Objects.equals(o1, o2);
                }

                // get value of object 1
                Object o1Value;
                try {
                    o1Value = PropertyUtils.getSimpleProperty(o1, field.getName());
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    if (logger.isDebugEnabled())
                        logger.debug("Can't access "+field.getName()+" on object 1. Ignoring.");
                    continue;
                }

                // get value of object 2
                Object o2Value;
                try {
                    o2Value = PropertyUtils.getSimpleProperty(o2, field.getName());
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    if (logger.isDebugEnabled())
                        logger.debug("Can't access "+field.getName()+" on object 2. Objects are inconsistent.");
                    return false;
                }

                // compare values
                if (o1Value instanceof Timestamp && o2Value instanceof Date || o1Value instanceof Date && o2Value instanceof Time) {
                    if (!Objects.equals(((Date)o1Value).getTime(), ((Date)o2Value).getTime())) {
                        logInconsistency(o1.getClass().getSimpleName()+" objects have "+field.getName()+" field inconsistent (checked java.sql.Timestamp against java.util.Date, ignoring nanoseconds)");
                        return false;
                    }
                }
                else {
                    if (!Objects.equals(o1Value, o2Value)) {
                        logInconsistency(o1.getClass().getSimpleName()+" objects have "+field.getName()+" field inconsistent");
                        return false;
                    }
                }

            }
        }

        logger.debug("Objects are consistent");
        return true;
    }

    private boolean consistencyCheckRequired(Field field) {

        // java 8 has field.getDeclaredAnnotation(clazz)
        ConsistencyCheck consistencyCheck = null;
        for (Annotation a: field.getDeclaredAnnotations()) {
            if (a instanceof ConsistencyCheck) {
                consistencyCheck = (ConsistencyCheck) a;
            }
        }

        if (consistencyCheck == null) {
            // check consistency by default
            return true;
        }

        if (consistencyCheck.ignore()) {
            if(logger.isDebugEnabled())
                logger.debug("Ignoring "+field.getName());
            return false;
        }

        return true;
    }

    private void logInconsistency(String message) {
        if (logInconsistenciesAsWarnings) {
            logger.warn(message);
        }
        else {
            logger.debug(message);
        }
    }

    private static BeanConsistencyChecker beanConsistencyChecker = new BeanConsistencyChecker();

    public static BeanConsistencyChecker getInstance() {
        return beanConsistencyChecker;
    }

}
