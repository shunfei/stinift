package com.sf.stinift;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sf.stinift.config.PluginRegister;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.plugin.mongo.MongoReader;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.mongo.MongoConnection;

/**
 * Created by scut_DELL on 15/12/9.
 */
public class MongoPlugin implements PluginRegister {
    @Override
    public void register(SimpleModule pluginModule, SimpleModule resourceModule) {
        pluginModule.addAbstractTypeMapping(Reader.Creator.class, MongoReader.Creator.class);
        pluginModule.registerSubtypes(new NamedType(MongoReader.Creator.class, "mongo"));

        resourceModule.addAbstractTypeMapping(Resource.Creator.class, MongoConnection.Creator.class);
        resourceModule.registerSubtypes(new NamedType(MongoConnection.Creator.class, "mongo"));
    }
}
