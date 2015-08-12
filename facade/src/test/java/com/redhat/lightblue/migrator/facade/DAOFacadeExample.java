package com.redhat.lightblue.migrator.facade;

import javax.management.RuntimeErrorException;

import com.redhat.lightblue.migrator.facade.DAOFacadeBase;
import com.redhat.lightblue.migrator.facade.EntityIdExtractor;
import com.redhat.lightblue.migrator.facade.model.Country;



public class DAOFacadeExample extends DAOFacadeBase<CountryDAO> implements CountryDAO {

    public final EntityIdExtractor<Country> entityIdExtractor = new EntityIdExtractor<Country>() {
        @Override
        public Long extractId(Country entity) {
            return entity.getId();
        }
    };

    public DAOFacadeExample(CountryDAO legacyDAO, CountryDAO lightblueDAO) {
        super(legacyDAO, lightblueDAO);
    }

    @Override
    public Country createCountry(Country country) throws CountryException {
        try {
            return callDAOCreateSingleMethod(true, entityIdExtractor, Country.class, "createCountry", country);
        } catch (CountryException ce) {
            throw ce;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public Country createCountryIfNotExists(Country country) throws CountryException {
        try {
            return callDAOCreateSingleMethod(false, entityIdExtractor, Country.class, "createCountryIfNotExists", country);
        } catch (CountryException ce) {
            throw ce;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public Country updateCountry(Country country) throws CountryException {
        try {
            return callDAOUpdateMethod(Country.class, "updateCountry", country);
        } catch (CountryException ce) {
            throw ce;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    @Override
    public Country getCountry(String iso2Code) throws CountryException {
        try {
            return callDAOReadMethod(Country.class, "getCountry", iso2Code);
        } catch (CountryException ce) {
            throw ce;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

}
