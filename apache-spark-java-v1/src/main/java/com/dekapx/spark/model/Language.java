package com.dekapx.spark.model;

public class Language {
    public Language() {
    }

    public Language(String name, LanguageType type) {
        this.name = name;
        this.type = type;
    }

    private String name;
    private LanguageType type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LanguageType getType() {
        return type;
    }

    public void setType(LanguageType type) {
        this.type = type;
    }
}
