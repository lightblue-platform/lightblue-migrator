package com.redhat.lightblue.migrator.utils;

/**
 * Extract ID of any entity. Use to define anonymous classes.
 *
 * @author mpatercz
 *
 * @param <E>
 */
public interface EntityIdExtractor<E> {
    public Long extractId(E entity);
}
