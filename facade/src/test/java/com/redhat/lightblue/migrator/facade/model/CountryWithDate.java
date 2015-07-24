package com.redhat.lightblue.migrator.facade.model;

import java.util.Date;

public class CountryWithDate extends Country {

    private Date visitDate;

    public CountryWithDate(Date visitDate) {
        this.visitDate = visitDate;
    }

    public Date getVisitDate() {
        return visitDate;
    }

    public void setVisitDate(Date visitDate) {
        this.visitDate = visitDate;
    }
}
