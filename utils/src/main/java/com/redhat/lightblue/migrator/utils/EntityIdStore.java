package com.redhat.lightblue.migrator.utils;

public interface EntityIdStore {

    public void storeId(Long id);

    public Long restoreId();

}
