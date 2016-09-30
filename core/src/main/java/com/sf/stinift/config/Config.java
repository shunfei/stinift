package com.sf.stinift.config;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sf.stinift.exchange.BlockingQueueExchanger;
import com.sf.stinift.exchange.Exchanger;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.CountingWriter;
import com.sf.stinift.plugin.NoneWriter;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.plugin.file.RowFileReader;
import com.sf.stinift.plugin.file.RowFileWriter;
import com.sf.stinift.plugin.test.TestReader;
import com.sf.stinift.plugin.test.TestWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import io.druid.guice.PolyBind;

public class Config {
    private static final Logger log = new Logger(Config.class);

    public static Properties properties;
    public static ObjectMapper jsonMapper;
    public static Injector injector;
    public static SystemConfig systemConfig;

    public static void init() {
        init(null, null);
    }

    public static void init(
            Iterable<Module> guiceModules,
            Iterable<com.fasterxml.jackson.databind.Module> jacksonModules
    ) {
        properties = readProperties("stinift_base.properties", "stinift.properties");

        if (guiceModules != null) {
            guiceModules = Iterables.concat(guiceModules, getDefaultModules());
        } else {
            guiceModules = getDefaultModules();
        }
        if (jacksonModules != null) {
            jacksonModules = Iterables.concat(jacksonModules, getDefaultJacksonModules());
        } else {
            jacksonModules = getDefaultJacksonModules();
        }

        injector = Guice.createInjector(guiceModules);

        jsonMapper = new ObjectMapper();
        jsonMapper.registerModules(jacksonModules);
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        systemConfig = injector.getInstance(SystemConfig.class);
    }

    private static Properties readProperties(String... propertiesFiles) {
        final Properties fileProps = new Properties();
        Properties systemProps = System.getProperties();

        Properties props = new Properties(fileProps);
        props.putAll(systemProps);

        for (String propertiesFile : propertiesFiles) {
            InputStream stream = ClassLoader.getSystemResourceAsStream(propertiesFile);
            try {
                if (stream != null) {
                    log.info("Loading properties from %s", propertiesFile);
                    try {
                        fileProps.load(new InputStreamReader(stream, Charsets.UTF_8));
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            } catch (Exception e) {
                log.wtf(e, "This can only happen if the .exists() call lied.  That's f'd up.");
            } finally {
                IOUtils.closeQuietly(stream);
            }
        }
        return props;
    }

    private static List<Module> getDefaultModules() {
        return ImmutableList.<Module>of(
                new Module() {
                    @Override
                    public void configure(Binder binder) {
                        // Default Exchanger
                        PolyBind.createChoice(
                                binder,
                                "stinift.exchanger.type",
                                Key.get(Exchanger.Creator.class),
                                Key.get(BlockingQueueExchanger.Creator.class)
                        );
                        // More choices
                        MapBinder<String, Exchanger.Creator> exchangerBinder = PolyBind.optionBinder(
                                binder,
                                Key.get(Exchanger.Creator.class)
                        );
                        exchangerBinder.addBinding("blockingQueue").to(BlockingQueueExchanger.Creator.class);
                    }
                },
                new Module() {
                    @Override
                    public void configure(Binder binder) {
                        binder.bind(Properties.class).toInstance(properties);
                        Names.bindProperties(binder, properties);
                    }
                }
        );
    }

    private static List<com.fasterxml.jackson.databind.Module> getDefaultJacksonModules() {
        SimpleModule pluginModule = new SimpleModule("PluginModule");
        SimpleModule resourceModule = new SimpleModule("ResourceModule");

        pluginModule
                .addAbstractTypeMapping(Reader.Creator.class, TestReader.Creator.class)
                .addAbstractTypeMapping(Writer.Creator.class, TestWriter.Creator.class)

                .addAbstractTypeMapping(Writer.Creator.class, NoneWriter.Creator.class)

                .addAbstractTypeMapping(Writer.Creator.class, CountingWriter.Creator.class)

                .addAbstractTypeMapping(Reader.Creator.class, RowFileReader.Creator.class)
                .addAbstractTypeMapping(Writer.Creator.class, RowFileWriter.Creator.class)

                .registerSubtypes(
                        new NamedType(TestReader.Creator.class, "test"),
                        new NamedType(TestWriter.Creator.class, "test"),

                        new NamedType(NoneWriter.Creator.class, "none"),

                        new NamedType(RowFileReader.Creator.class, "rowfile"),
                        new NamedType(RowFileWriter.Creator.class, "rowfile"),

                        new NamedType(CountingWriter.Creator.class, "counting")
                );

        String[] pluginClasses = StringUtils.split(properties.getProperty(PLUGIN_CLASSES), ',');
        for (String pluginClass : pluginClasses) {
            try {
                PluginRegister pluginRegister = (PluginRegister) Class.forName(pluginClass).newInstance();
                pluginRegister.register(pluginModule, resourceModule);
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        return ImmutableList.<com.fasterxml.jackson.databind.Module>of(pluginModule, resourceModule);
    }


    public static String ARRAY_DELIMITER = "stinift.array.delimiter";
    public static String RESOURCE_FACTORY = "stinift.resource.factory";
    public static String PLUGIN_CLASSES = "stinift.plugin.classes";
}
