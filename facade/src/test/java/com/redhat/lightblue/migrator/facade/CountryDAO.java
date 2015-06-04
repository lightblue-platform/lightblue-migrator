package com.redhat.lightblue.migrator.facade;

public interface CountryDAO {

    public abstract Country createCountry(Country country);

    public abstract Country createCountryIfNotExists(Country country);

    public abstract Country updateCountry(Country country);

    public abstract Country getCountry(String iso2Code);

}