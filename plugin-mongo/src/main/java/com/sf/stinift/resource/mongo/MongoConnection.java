package com.sf.stinift.resource.mongo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.sf.stinift.resource.Resource;

import org.apache.commons.io.IOUtils;

public class MongoConnection extends Resource {
    private final String host;
    private final Integer port;
    private final String db;
    private final String user;
    private final String pwd;

    private MongoClient mongoClient;

    MongoConnection(String host, Integer port, String db, String user, String pwd) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.pwd = pwd;
        this.user = user;
    }

    public DB getDataSource() {
        return mongoClient.getDB(db);
    }

    @Override
    public void open() throws Exception {
        mongoClient = new MongoClient(host, port);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(mongoClient);
    }

    @Override
    public String info() {
        return String.format("host: %s, port: %d, db: %s", host, port, db);
    }

    public static class Creator extends Resource.Creator {
        @JsonProperty
        public String host;
        @JsonProperty
        public Integer port;
        @JsonProperty
        public String db;
        @JsonProperty
        public String user;
        @JsonProperty
        public String pwd;

        @SuppressWarnings("unchecked")
        @Override
        public MongoConnection create() {
            return new MongoConnection(host, port, db, user, pwd);
        }
    }
}
