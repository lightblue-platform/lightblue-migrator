package com.redhat.lightblue.migrator.facade.model;

public class VeryExtendedCountry extends ExtendedCountry {

    public VeryExtendedCountry(Long id, String iso2Code, String inaccessibleFieldConsistencyReq) {
        super(id, iso2Code);
        this.inaccessibleFieldConsistencyReq = inaccessibleFieldConsistencyReq;
    }

    private String inaccessibleFieldConsistencyReq;

}
