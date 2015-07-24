package com.redhat.lightblue.migrator.facade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use <code>ModelMixIn(clazz=Model.class)</code> to map a model object to the jackson Mix-In interface
 * for overriding jackson annotations during bean serialization.
 *
 * @author ykoer
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ModelMixIn {
    Class clazz();
}
