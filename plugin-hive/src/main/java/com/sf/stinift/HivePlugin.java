package com.sf.stinift;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sf.stinift.config.PluginRegister;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.plugin.hive.HiveReader;
import com.sf.stinift.plugin.hive.HiveWriter;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.hive.HiveConnection;

/**
 * Created by scut_DELL on 15/12/9.
 */
public class HivePlugin implements PluginRegister {
    @Override
    public void register(SimpleModule pluginModule, SimpleModule resourceModule) {
        pluginModule.addAbstractTypeMapping(Reader.Creator.class, HiveReader.Creator.class);
        pluginModule.registerSubtypes(new NamedType(HiveReader.Creator.class, "hive"));

        pluginModule.addAbstractTypeMapping(Writer.Creator.class, HiveWriter.Creator.class);
        pluginModule.registerSubtypes(new NamedType(HiveWriter.Creator.class, "hive"));

        resourceModule.addAbstractTypeMapping(Resource.Creator.class, HiveConnection.Creator.class);
        resourceModule.registerSubtypes(new NamedType(HiveConnection.Creator.class, "hive"));
    }
}
