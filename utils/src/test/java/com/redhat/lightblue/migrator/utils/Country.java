package com.redhat.lightblue.migrator.utils;

public class Country {

    private String name, iso2Code, iso3Code;

    public Country(String iso2Code) {
        super();
        this.iso2Code = iso2Code;
    }

    public Country() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIso2Code() {
        return iso2Code;
    }

    public void setIso2Code(String iso2code) {
        this.iso2Code = iso2code;
    }

    public String getIso3Code() {
        return iso3Code;
    }

    public void setIso3Code(String iso3code) {
        this.iso3Code = iso3code;
    }

    @Override
    public boolean equals(Object obj) {
        Country c = (Country) obj;

        if (iso2Code == null && c.iso2Code == null) {
            return true;
        }

        try {
            return iso2Code.equals(c.iso2Code);
        } catch (NullPointerException e) {
            return false;
        }
    }

}
