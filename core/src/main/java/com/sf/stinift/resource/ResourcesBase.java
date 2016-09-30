package com.sf.stinift.resource;

import com.sf.stinift.config.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by scut_DELL on 15/12/9.
 */
public class ResourcesBase implements Resources {

    Map<String, Resource> resources;

    private ResourcesBase() {}

    public void setUserResources(Map<String, Resource> resources) {
        this.resources = resources;
        if (this.resources == null) {
            this.resources = new HashMap<String, Resource>();
        }
    }

    @Override
    public Resource getResourceByName(String name) {
        return resources.get(name);
    }

    public static ResourcesBase create(Map<String, Resource> resources) {
        String clazz = Config.properties.getProperty(Config.RESOURCE_FACTORY);
        ResourcesBase resourcesBase = null;
        try {
            resourcesBase = (ResourcesBase) Class.forName(clazz).newInstance();
            resourcesBase.setUserResources(resources);
        } catch (InstantiationException e) {

        } catch (IllegalAccessException e) {

        } catch (ClassNotFoundException e) {

        }
        return resourcesBase;
    }
}
