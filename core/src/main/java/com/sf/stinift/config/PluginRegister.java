package com.sf.stinift.config;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Created by scut_DELL on 15/12/9.
 */
public interface PluginRegister {
    void register(SimpleModule pluginModule, SimpleModule resourceModule);
}
