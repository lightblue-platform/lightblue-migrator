package com.redhat.lightblue.migrator.facade.model;

public class ExtendedCountry extends Country {

    public ExtendedCountry(Long id, String iso2Code) {
        super(id, iso2Code);
    }

    private String additionalFieldConsistencyReq;
    private String additionalFieldConsistencyNotReq;

    public String getAdditionalFieldConsistencyReq() {
        return additionalFieldConsistencyReq;
    }

    public void setAdditionalFieldConsistencyReq(String additionalFieldConsistencyReq) {
        this.additionalFieldConsistencyReq = additionalFieldConsistencyReq;
    }

    public String getAdditionalFieldConsistencyNotReq() {
        return additionalFieldConsistencyNotReq;
    }

    public void setAdditionalFieldConsistencyNotReq(String additionalFieldConsistencyNotReq) {
        this.additionalFieldConsistencyNotReq = additionalFieldConsistencyNotReq;
    }

}
