package com.redhat.lightblue.migrator.facade.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.redhat.lightblue.migrator.facade.ModelMixIn;

@ModelMixIn(clazz=Person.class, includeMethods="getPerson2")
public interface Person2MixIn {
    @JsonIgnore Integer getAge();
}
