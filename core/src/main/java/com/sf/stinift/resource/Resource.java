package com.sf.stinift.resource;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.sf.stinift.utils.Infoable;
import com.sf.stinift.utils.JsonInheritable;

import java.io.Closeable;

public abstract class Resource implements Closeable, Infoable {
    private String name;

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract void open() throws Exception;

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    public static abstract class Creator extends JsonInheritable.Class {

        public abstract <T extends Resource> T create();

    }

}
