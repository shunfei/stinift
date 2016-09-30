package com.sf.stinift;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sf.stinift.config.PluginRegister;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.plugin.hbase.HBaseWriter;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.hbase.HBaseConnection;

/**
 * Created by scut_DELL on 15/12/9.
 */
public class HbasePlugin implements PluginRegister {
    @Override
    public void register(SimpleModule pluginModule, SimpleModule resourceModule) {
        pluginModule.addAbstractTypeMapping(Writer.Creator.class, HBaseWriter.Creator.class);
        pluginModule.registerSubtypes(new NamedType(HBaseWriter.Creator.class, "hbase"));

        resourceModule.addAbstractTypeMapping(Resource.Creator.class, HBaseConnection.Creator.class);
        resourceModule.registerSubtypes(new NamedType(HBaseConnection.Creator.class, "hbase"));
    }
}
