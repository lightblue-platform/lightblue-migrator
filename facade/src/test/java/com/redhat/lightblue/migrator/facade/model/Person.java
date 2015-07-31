package com.redhat.lightblue.migrator.facade.model;

public class Person {

    public Person() {
    }

    public Person(String firstName, String lastName, Integer age, String citizenship) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.citizenship = citizenship;
    }

    private String firstName;
    private String lastName;
    private Integer age;
    private String  citizenship;

    public String getFirstName() {
        return firstName;
    }
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    public String getLastName() {
        return lastName;
    }
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    public Integer getAge() {
        return age;
    }
    public void setAge(Integer age) {
        this.age = age;
    }
    public String getCitizenship() {
        return citizenship;
    }
    public void setCitizenship(String citizenship) {
        this.citizenship = citizenship;
    }
}
