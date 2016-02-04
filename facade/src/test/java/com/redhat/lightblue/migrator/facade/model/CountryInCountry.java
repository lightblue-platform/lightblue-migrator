package com.redhat.lightblue.migrator.facade.model;


public class CountryInCountry extends Country {

    private Country country;

    public CountryInCountry(Long id, String iso2Code, Country country) {
        super(id, iso2Code);
        this.country = country;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

}
