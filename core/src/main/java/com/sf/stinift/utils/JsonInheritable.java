package com.sf.stinift.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public interface JsonInheritable {
    public String name();

    public String inherit();

    public static class Class implements JsonInheritable {
        @JsonProperty
        public String name;
        @JsonProperty
        public String inherit;

        @Override
        public String name() {
            return name;
        }

        @Override
        public String inherit() {
            return inherit;
        }
    }
}
