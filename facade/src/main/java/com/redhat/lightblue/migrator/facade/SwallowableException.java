package com.redhat.lightblue.migrator.facade;

/**
 * A wrapper around exceptions indicating if they should be swallowed by the facade.
 * Swallowing means logging the exception and fail-overing to source implementation result.
 *
 * @author mpatercz
 *
 */
public class SwallowableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public final boolean swallow;

    public SwallowableException(Throwable cause, boolean swallow) {
        super(cause);
        this.swallow = swallow;
    }

}
