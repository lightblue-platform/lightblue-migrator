package com.redhat.lightblue.migrator.facade.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.redhat.lightblue.migrator.facade.ModelMixIn;

@ModelMixIn(clazz=Country.class)
public interface CountryMixIn {

    @JsonIgnore Long getName();
}
