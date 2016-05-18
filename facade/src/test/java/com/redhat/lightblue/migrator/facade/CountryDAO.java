package com.redhat.lightblue.migrator.facade;

import java.util.List;

import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.Secret;

/**
 * Interface of a service to be migrated to Lightblue.
 *
 * @author mpatercz
 *
 */
public interface CountryDAO {

    public abstract Country createCountry(Country country) throws CountryException;

    public abstract Country createCountryIfNotExists(Country country) throws CountryException;

    public abstract Country updateCountry(Country country) throws CountryException;

    public abstract Country getCountry(String iso2Code) throws CountryException;

    public abstract List<Country> getCountries(long[] ids) throws CountryException;

    public abstract Country getCountryFromLegacy(long id) throws CountryException;

    public abstract Country getCountryFromLegacy2(long id) throws CountryException;

    public abstract Country getCountryFromLightblue(long id) throws CountryException;

    public abstract Country createGeneratedCountry() throws CountryException;

    public abstract Country secureCountry(long id, @Secret String password) throws CountryException;

}
