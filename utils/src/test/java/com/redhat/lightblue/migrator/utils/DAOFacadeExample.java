package com.redhat.lightblue.migrator.utils;


public class DAOFacadeExample extends DAOFacadeBase<CountryDAO> implements CountryDAO {

    public DAOFacadeExample(CountryDAO legacyDAO, CountryDAO lightblueDAO) {
        super(legacyDAO, lightblueDAO);
    }

    @Override
    public Country createCountry(Country country) {
        try {
            return callDAOWriteMethod(Country.class, "createCountry", country);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Country updateCountry(Country country) {
        try {
            return callDAOWriteMethod(Country.class, "updateCountry", country);
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
