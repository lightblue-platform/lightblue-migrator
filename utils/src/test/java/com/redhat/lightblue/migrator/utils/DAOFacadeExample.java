package com.redhat.lightblue.migrator.utils;


public class DAOFacadeExample extends DAOFacadeBase<CountryDAO> implements CountryDAO {

    public final EntityIdExtractor<Country> entityIdExtractor = new EntityIdExtractor<Country>() {
        @Override
        public Long extractId(Country entity) {
            return entity.getId();
        }
    };

    public DAOFacadeExample(CountryDAO legacyDAO, CountryDAO lightblueDAO) {
        super(legacyDAO, lightblueDAO, Country.class);
    }

    @Override
    public Country createCountry(Country country) {
        try {
            return callDAOCreateSingleMethod(entityIdExtractor, Country.class, "createCountry", country);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Country updateCountry(Country country) {
        try {
            return callDAOUpdateMethod(Country.class, "updateCountry", country);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Country getCountry(String iso2Code) {
        try {
            return callDAOReadMethod(Country.class, "getCountry", iso2Code);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
