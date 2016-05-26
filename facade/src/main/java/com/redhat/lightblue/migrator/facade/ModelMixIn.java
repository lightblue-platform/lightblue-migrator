package com.redhat.lightblue.migrator.facade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use
 * <code>ModelMixIn(clazz=Model.class, includeMethods={"apiMethod1","apiMethod2"})</code>
 * to exclude certain fields from the serialized json before consistency
 * checking.
 *
 *
 * @author ykoer
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ModelMixIn {
    /**
     * The MixIn interface/class for a model object.
     *
     * @return
     */
    Class clazz();
}
