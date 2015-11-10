package com.redhat.lightblue.migrator.facade;

import java.util.List;

import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.ReadOperation;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.UpdateOperation;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.WriteSingleOperation;

public interface CountryDAO {

    @WriteSingleOperation(entityIdExtractorClassName = "com.redhat.lightblue.migrator.facade.CountryIdExtractor")
    public abstract Country createCountry(Country country) throws CountryException;

    @WriteSingleOperation(entityIdExtractorClassName = "com.redhat.lightblue.migrator.facade.CountryIdExtractor")
    public abstract Country createCountryIfNotExists(Country country) throws CountryException;

    @UpdateOperation
    public abstract Country updateCountry(Country country) throws CountryException;

    @ReadOperation
    public abstract Country getCountry(String iso2Code) throws CountryException;

    @ReadOperation
    public abstract List<Country> getCountries(long[] ids) throws CountryException;

    public abstract Country getCountryFromLegacy(long id);

}