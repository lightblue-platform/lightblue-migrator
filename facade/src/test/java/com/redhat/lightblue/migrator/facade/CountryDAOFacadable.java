package com.redhat.lightblue.migrator.facade;

import java.util.List;

import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.DirectOperation;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.ReadOperation;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.Secret;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.WriteOperation;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.DirectOperation.Target;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreSetter;

/**
 * Service interface with facade configurations.
 *
 * @author mpatercz
 *
 */
public interface CountryDAOFacadable extends CountryDAO, SharedStoreSetter {

    @WriteOperation
    public abstract Country createCountry(Country country) throws CountryException;

    @WriteOperation
    public abstract Country createCountryIfNotExists(Country country) throws CountryException;

    @WriteOperation(parallel=true)
    public abstract Country updateCountry(Country country) throws CountryException;

    @ReadOperation(parallel=true)
    public abstract Country getCountry(String iso2Code) throws CountryException;

    @ReadOperation(parallel=true)
    public abstract List<Country> getCountries(long[] ids) throws CountryException;

    public abstract Country getCountryFromLegacy(long id) throws CountryException;

    @DirectOperation(target=Target.LEGACY)
    public abstract Country getCountryFromLegacy2(long id) throws CountryException;

    @DirectOperation(target=Target.LIGHTBLUE)
    public abstract Country getCountryFromLightblue(long id) throws CountryException;

    @WriteOperation
    public abstract Country createGeneratedCountry() throws CountryException;

    /**
     * Shows how to avoid logging sensitive information passed as parameters.
     *
     */
    @WriteOperation
    public abstract Country secureCountry(long id, @Secret String password) throws CountryException;

}