package com.redhat.lightblue.migrator.utils;

public interface CountryDAO {

    public abstract Country createCountry(Country country);

    public abstract Country updateCountry(Country country);

    public abstract Country getCountry(String iso2Code);

}