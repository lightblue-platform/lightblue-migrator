package com.redhat.lightblue.migrator.consistency;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use <code>ConsistencyCheck(ignore=true)</code> to flag fields which are not to be checked by {@link BeanConsistencyChecker}.
 *
 * @author mpatercz
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface ConsistencyCheck {
    boolean ignore() default false;
}
