package com.redhat.lightblue.migrator.facade;

import com.redhat.lightblue.migrator.facade.model.Country;

public interface CountryDAO {

    public abstract Country createCountry(Country country) throws CountryException;

    public abstract Country createCountryIfNotExists(Country country) throws CountryException;

    public abstract Country updateCountry(Country country) throws CountryException;

    public abstract Country getCountry(String iso2Code) throws CountryException;

    public abstract Country getCountries(long[] ids) throws CountryException;

}