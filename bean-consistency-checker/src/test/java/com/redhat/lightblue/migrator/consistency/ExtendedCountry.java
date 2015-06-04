package com.redhat.lightblue.migrator.consistency;


public class ExtendedCountry extends Country {

    public ExtendedCountry(Long id, String iso2Code) {
        super(id, iso2Code);
    }

    private String additionalFieldConsistencyReq;

    @ConsistencyCheck(ignore=true)
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
