package com.sf.stinift;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sf.stinift.config.PluginRegister;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.plugin.mysql.MysqlReader;
import com.sf.stinift.plugin.mysql.MysqlWriter;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.mysql.MysqlConnection;

/**
 * Created by scut_DELL on 15/12/9.
 */
public class MysqlPlugin implements PluginRegister {
    @Override
    public void register(SimpleModule pluginModule, SimpleModule resourceModule) {
        pluginModule.addAbstractTypeMapping(Reader.Creator.class, MysqlReader.Creator.class);
        pluginModule.registerSubtypes(new NamedType(MysqlReader.Creator.class, "mysql"));

        pluginModule.addAbstractTypeMapping(Writer.Creator.class, MysqlWriter.Creator.class);
        pluginModule.registerSubtypes(new NamedType(MysqlWriter.Creator.class, "mysql"));

        resourceModule.addAbstractTypeMapping(Resource.Creator.class, MysqlConnection.Creator.class);
        resourceModule.registerSubtypes(new NamedType(MysqlConnection.Creator.class, "mysql"));
    }
}
