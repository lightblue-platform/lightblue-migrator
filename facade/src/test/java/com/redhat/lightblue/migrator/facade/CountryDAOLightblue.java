package com.redhat.lightblue.migrator.facade;

import com.redhat.lightblue.migrator.facade.EntityIdStore;

public interface CountryDAOLightblue extends CountryDAO {

    public abstract void setEntityIdStore(EntityIdStore store);

}
