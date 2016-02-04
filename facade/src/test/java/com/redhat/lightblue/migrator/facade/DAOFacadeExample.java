package com.redhat.lightblue.migrator.facade;

import java.util.List;

import com.redhat.lightblue.migrator.facade.model.Country;

@Deprecated
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
            return callDAOCreateSingleMethod(entityIdExtractor, Country.class, "createCountry", country);
        } catch (CountryException ce) {
            throw ce;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public Country createCountryIfNotExists(Country country) throws CountryException {
        try {
            return callDAOCreateSingleMethod(entityIdExtractor, Country.class, "createCountryIfNotExists", country);
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

    @Override
    public List<Country> getCountries(long[] ids) throws CountryException {
        try {
            return callDAOReadMethod(List.class, "getCountries", ids);
        } catch (CountryException ce) {
            throw ce;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public Country getCountryFromLegacy(long id) throws CountryException {
        return legacyDAO.getCountryFromLegacy(id);
    }

    @Override
    public Country createGeneratedCountry() {
        throw new UnsupportedOperationException("Unsupported in legacy facade");
    }

    @Override
    public Country secureCountry(long id, String password) throws CountryException {
        throw new UnsupportedOperationException("Unsupported in legacy facade");
    }

}
