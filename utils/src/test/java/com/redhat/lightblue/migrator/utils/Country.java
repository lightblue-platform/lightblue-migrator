package com.redhat.lightblue.migrator.utils;

import java.util.Objects;

public class Country {

    private String name, iso2Code, iso3Code;
    private Long id;

    public long getId() {
        return id;
    }

    public Country(String iso2Code) {
        this(null, iso2Code);
    }

    public Country(Long id, String iso2Code) {
        super();
        this.id = id;
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

        return Objects.equals(c.iso2Code, iso2Code) && Objects.equals(c.id, id);
    }

}
