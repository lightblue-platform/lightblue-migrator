package com.redhat.lightblue.migrator.facade;

import com.redhat.lightblue.migrator.facade.model.Country;

public class CountryIdExtractor implements EntityIdExtractor<Country>{
    @Override
    public Long extractId(Country entity) {
        return entity.getId();
    }
}
