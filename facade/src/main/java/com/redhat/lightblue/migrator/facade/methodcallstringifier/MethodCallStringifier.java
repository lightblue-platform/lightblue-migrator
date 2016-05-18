package com.redhat.lightblue.migrator.facade.methodcallstringifier;

/**
 * Generates method(arg1, arg2, ...) string used to trace errors and data
 * inconsistency warnings to particular method call.
 *
 * @author mpatercz
 *
 */
public interface MethodCallStringifier {

    public abstract String toString();

}
