package com.sf.stinift.config;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.JobParam;
import com.sf.stinift.exchange.Exchanger;
import com.sf.stinift.plugin.JobParamErrorException;
import com.sf.stinift.plugin.NoneWriter;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.utils.JsonUtils;
import com.sf.stinift.utils.Utils;
import com.sf.stinift.worker.ReaderWorker;
import com.sf.stinift.worker.Worker;
import com.sf.stinift.worker.WriterWorker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

public class JobConfigParser {
    public static JobParam parseConfig(String path) throws Exception {
        List<String> filePaths = Utils.wildcardFiles(path);
        JobJsonConfig config = JobJsonConfig.merge(
                Lists.transform(
                        filePaths, new Function<String, JobJsonConfig>() {
                            @Nullable
                            @Override
                            public JobJsonConfig apply(String s) {
                                try {
                                    return JsonUtils.mapJson(s, JobJsonConfig.class);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                )
        );

        final Map<String, Reader.Creator> readerMap = Maps.uniqueIndex(
                config.readers, new Function<Reader.Creator, String>() {
                    @Override
                    public String apply(Reader.Creator input) {
                        return input.name();
                    }
                }
        );
        final Map<String, Writer.Creator> writerMap = Maps.uniqueIndex(
                config.writers, new Function<Writer.Creator, String>() {
                    @Override
                    public String apply(Writer.Creator input) {
                        return input.name();
                    }
                }
        );
        JsonUtils.fillInherit(readerMap, config.readers);
        JsonUtils.fillInherit(writerMap, config.writers);
        List<Route> routes = new ArrayList<>();
        for (int i = 0; i < config.routes.size(); i++) {
            routes.add(new Route(i, config.routes.get(i), readerMap, writerMap));
        }
        List<Worker> workers = Lists.newArrayList();
        for (Route route : routes) {
            workers.addAll(route.toWorkers());
        }

        Map<String, Resource> resources;
        if (config.resources == null) {
            resources = Collections.emptyMap();
        } else {
            resources = Maps.uniqueIndex(
                    Lists.transform(
                            config.resources,
                            new Function<Resource.Creator, Resource>() {
                                @Nullable
                                @Override
                                public Resource apply(Resource.Creator input) {
                                    Resource res = input.create();
                                    res.setName(input.name());
                                    return res;
                                }
                            }
                    ), new Function<Resource, String>() {
                        @Nullable
                        @Override
                        public String apply(Resource input) {
                            return input.name();
                        }
                    }
            );
        }
        return new JobParam(resources, workers);
    }

    public static class JobJsonConfig {
        @JsonProperty
        public List<Resource.Creator> resources;
        @JsonProperty
        public List<Reader.Creator> readers;
        @JsonProperty
        public List<Writer.Creator> writers;
        @JsonProperty
        public List<String> routes;

        public static JobJsonConfig merge(List<JobJsonConfig> configs) {
            JobJsonConfig merged = new JobJsonConfig();
            merged.resources = new ArrayList<>();
            merged.readers = new ArrayList<>();
            merged.writers = new ArrayList<>();
            merged.routes = new ArrayList<>();

            for (JobJsonConfig config : configs) {
                if (config.resources != null) {
                    merged.resources.addAll(config.resources);
                }
                if (config.readers != null) {
                    merged.readers.addAll(config.readers);
                }
                if (config.writers != null) {
                    merged.writers.addAll(config.writers);
                }
                if (config.routes != null) {
                    merged.routes.addAll(config.routes);
                }
            }
            return merged;
        }
    }

    public static class Route {
        private int index;
        private String expr;

        public List<Reader.Creator> readers;
        public List<Writer.Creator> writers;

        public Route(
                int index,
                String expr,
                final Map<String, Reader.Creator> readerMap,
                final Map<String, Writer.Creator> writerMap
        ) {
            this.index = index;
            this.expr = expr;
            String[] ss = expr.trim().split(">");
            if (ss.length <= 0) {
                throw new RuntimeException(String.format("route expression [%s] illegal", expr));
            }
            readers = Lists.transform(
                    Arrays.asList(ss[0].trim().split(",")), new Function<String, Reader.Creator>() {
                        @Override
                        public Reader.Creator apply(String name) {
                            Reader.Creator reader = readerMap.get(name.trim());
                            if (reader == null) {
                                throw new RuntimeException(String.format("reader [%s] not exists", name.trim()));
                            }
                            return reader;
                        }
                    }
            );
            if (ss.length == 1) {
                writers = ImmutableList.<Writer.Creator>of(new NoneWriter.Creator());
            } else {
                writers = Lists.transform(
                        Arrays.asList(ss[1].trim().split(",")), new Function<String, Writer.Creator>() {
                            @Override
                            public Writer.Creator apply(String name) {
                                Writer.Creator writer = writerMap.get(name.trim());
                                if (writer == null) {
                                    throw new RuntimeException(String.format("writer [%s] not exists", name.trim()));
                                }
                                return writer;
                            }
                        }
                );
            }
        }

        public List<Worker> toWorkers() throws JobParamErrorException {
            Exchanger.Creator exCreator = Config.injector.getInstance(Exchanger.Creator.class);
            List<Worker> workers = Lists.newArrayList();
            for (Writer.Creator wCreator : writers) {
                Exchanger exchanger = exCreator.create();
                List<? extends Writer> writers = wCreator.create();
                for (Writer writer : writers) {
                    writer.setName(String.format("{%s} of {%s}", wCreator.name(), expr));
                    workers.add(new WriterWorker(writer, exchanger.getFetchable()));
                }
                for (Reader.Creator rCreator : readers) {
                    int rId = 0;
                    List<? extends Reader> readers = rCreator.create();
                    for (Reader reader : readers) {
                        reader.setName(String.format("{%s > %s} of {%s}", rCreator.name(), wCreator.name, expr));
                        workers.add(new ReaderWorker(reader, exchanger.getPushable()));
                        rId++;
                    }
                }
            }
            return workers;
        }
    }
}
