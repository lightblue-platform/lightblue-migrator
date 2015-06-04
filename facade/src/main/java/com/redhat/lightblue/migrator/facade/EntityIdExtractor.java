package com.redhat.lightblue.migrator.facade;

/**
 * Extract ID of any entity. Define anonymous class inside facade implementation for your particular DAO and entity.
 *
 * @author mpatercz
 *
 * @param <E>
 */
public interface EntityIdExtractor<E> {
    public Long extractId(E entity);
}
