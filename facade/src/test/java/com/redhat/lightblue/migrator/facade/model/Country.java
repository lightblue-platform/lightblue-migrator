package com.redhat.lightblue.migrator.facade.model;

import java.util.List;

import com.google.common.base.Objects;

public class Country {

    private String name;
    private String iso2Code, iso3Code;
    private Long id;
    private Country neighbour;
    private List<Country> allies;

    public Long getId() {
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
    public String toString() {
        return iso2Code + " id=" + id;
    }

    @Override
    public boolean equals(Object obj) {
        Country c = (Country) obj;

        if (c == null) {
            return false;
        }

        if (Objects.equal(id, c.getId()) && Objects.equal(iso2Code, c.getIso2Code()) && Objects.equal(iso3Code, c.getIso3Code())) {
            return true;
        }

        return false;
    }

    public Country getNeighbour() {
        return neighbour;
    }

    public void setNeighbour(Country neighbour) {
        this.neighbour = neighbour;
    }

    public List<Country> getAllies() {
        return allies;
    }

    public void setAllies(List<Country> allies) {
        this.allies = allies;
    }

}
