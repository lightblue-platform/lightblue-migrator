package com.redhat.lightblue.migrator.facade.model;

import java.math.BigDecimal;

public class CountryWithBigDecimal {

    private BigDecimal totalAreaInSquareKM;

    public CountryWithBigDecimal(BigDecimal totalAreaInSquareKM) {
        this.totalAreaInSquareKM = totalAreaInSquareKM;
    }

    public BigDecimal getTotalAreaInSquareKM() {
        return totalAreaInSquareKM;
    }

    public void setTotalAreaInSquareKM(BigDecimal totalAreaInSquareKM) {
        this.totalAreaInSquareKM = totalAreaInSquareKM;
    }
}
