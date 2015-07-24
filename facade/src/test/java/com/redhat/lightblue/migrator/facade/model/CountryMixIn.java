package com.redhat.lightblue.migrator.facade.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.redhat.lightblue.migrator.facade.ModelMixIn;

/**
 * Interface used for defining the mapping between a model object and a MixIn interface.
 * This gives us the possibility to override the Jackson serialization behavior.
 * For example to ignore consistency checking for certain fields.
 * All interfaces which are annotated with @ModelMixIn will be discovered at runtime by reflection.
 */
@ModelMixIn(clazz=Country.class)
public interface CountryMixIn {

    @JsonIgnore String getName();
}
