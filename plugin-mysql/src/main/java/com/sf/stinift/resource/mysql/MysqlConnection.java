package com.sf.stinift.resource.mysql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jolbox.bonecp.BoneCPDataSource;
import com.sf.stinift.resource.Resource;

import org.apache.commons.io.IOUtils;

import javax.sql.DataSource;

public class MysqlConnection extends Resource {
    private final String host;
    private final Integer port;
    private final String db;
    private final String user;
    private final String pwd;

    private BoneCPDataSource dataSource;

    MysqlConnection(String host, Integer port, String db, String user, String pwd) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.pwd = pwd;
        this.user = user;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public void open() throws Exception {
        dataSource = new BoneCPDataSource();
        dataSource.setDisableJMX(true);
        dataSource.setDriverClass("com.mysql.jdbc.Driver");
        dataSource.setJdbcUrl(
                String.format(
                        "jdbc:mysql://%s:%d/%s?",
                        host,
                        port,
                        db
                )
        );
        dataSource.setUser(user);
        dataSource.setPassword(pwd);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(dataSource);
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
        public MysqlConnection create() {
            return new MysqlConnection(host, port, db, user, pwd);
        }
    }
}
