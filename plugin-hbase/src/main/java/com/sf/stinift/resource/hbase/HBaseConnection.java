package com.sf.stinift.resource.hbase;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.resource.Resource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection extends Resource {
    private final String configPath;
    private Connection connection;

    public HBaseConnection(String configPath) {
        this.configPath = configPath;
    }

    @Override
    public String info() {
        return String.format("configPath: %s", configPath);
    }

    @Override
    public void open() throws Exception {
        Configuration config = HBaseConfiguration.create();
        if (!StringUtils.isEmpty(configPath)) {
            config.addResource(new Path(configPath));
        }
        connection = ConnectionFactory.createConnection(config);
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            IOUtils.closeQuietly(connection);
        }
    }

    public Connection connection() {
        return connection;
    }

    public static class Creator extends Resource.Creator {
        @JsonProperty
        public String configPath;

        @SuppressWarnings("unchecked")
        @Override
        public HBaseConnection create() {
            return new HBaseConnection(configPath);
        }
    }
}
