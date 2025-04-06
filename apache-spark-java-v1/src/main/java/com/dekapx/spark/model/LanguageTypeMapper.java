package com.dekapx.spark.model;

import java.util.Map;

import static com.dekapx.spark.model.LanguageType.*;


public class LanguageTypeMapper {
    private static Map<String, LanguageType> languageTypeMap = Map.of(
            "Java", FUNCTIONAL,
            "Python", SCRIPTING,
            "Scala", FUNCTIONAL,
            "C++", OBJECT_ORIENTED,
            "JavaScript", SCRIPTING
    );

    public static LanguageType getLanguageType(String language) {
        return languageTypeMap.getOrDefault(language, OBJECT_ORIENTED);
    }
}
