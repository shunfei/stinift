package com.sf.stinift.plugin.mysql;

import com.jolbox.bonecp.BoneCPDataSource;
import com.sf.stinift.log.Logger;

import org.apache.commons.io.IOUtils;

import java.sql.Connection;
import java.sql.SQLException;

class MysqlCommon {
    private static final Logger log = new Logger(MysqlReader.class);

    final String host;
    final Integer port;
    final String db;
    final String user;
    final String pwd;
    final String sql;

    BoneCPDataSource dataSource;
    Connection connection;

    MysqlCommon(String host, Integer port, String db, String user, String pwd, String sql) {
        this.db = db;
        this.host = host;
        this.port = port;
        this.pwd = pwd;
        this.sql = sql;
        this.user = user;
    }

    String info() {
        return String.format(
                "host:[%s], port[%d], db[%s], user[%s], pwd[%s], sql[%s]",
                host,
                port,
                db,
                user,
                pwd,
                sql
        );
    }

    void connect() throws Exception {
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

        connection = dataSource.getConnection();
    }

    void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error(e, "close error");
            }
        }
        IOUtils.closeQuietly(dataSource);
    }
}
