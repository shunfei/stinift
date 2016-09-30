package com.sf.stinift.plugin.mysql;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.JobParamErrorException;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.mysql.MysqlConnection;
import com.sf.stinift.utils.RowInputStream;
import com.sf.stinift.utils.Utils;
import com.sf.stinift.worker.WorkerContext;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class MysqlWriter extends Writer {
    private static final Logger log = new Logger(MysqlReader.class);

    private final String mysqlName;
    private final String sql;
    private final String pre;
    private final String post;

    private Connection connection;

    public MysqlWriter(String mysqlName, String sql, String pre, String post) {
        this.mysqlName = mysqlName;
        this.sql = sql;
        this.pre = pre;
        this.post = post;
    }

    @Override
    public String info() {
        return String.format(
                "mysql:[%s], sql[%s]",
                mysqlName,
                sql
        );
    }

    @Override
    public boolean start(WorkerContext context, Fetchable fetchable) throws Exception {
        Resource resource = context.getResource(mysqlName);
        if (resource == null) {
            throw new RuntimeException(String.format("mysql resource [%s]", mysqlName));
        }
        MysqlConnection mysqlConn = (MysqlConnection) resource;

        try {
            connection = mysqlConn.getDataSource().getConnection();

            if (StringUtils.isNotBlank(pre)) {
                executeSql(pre, connection);
            }

            Statement statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE
            );
            com.jolbox.bonecp.StatementHandle bonecpStat = (com.jolbox.bonecp.StatementHandle) statement;
            com.mysql.jdbc.Statement mysqlStat = (com.mysql.jdbc.Statement) bonecpStat.getInternalStatement();

            mysqlStat.setLocalInfileInputStream(new RowInputStream(fetchable));
            mysqlStat.executeUpdate(sql);

            if (StringUtils.isNotBlank(post)) {
                executeSql(post, connection);
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            Utils.closeQuietly(connection);
        }
    }

    private void executeSql(String sql, Connection connection) throws SQLException {
        log.info("start execute sql:" + sql);
        Statement statement = connection.createStatement();
        statement.execute(sql);
        statement.close();
        log.info("execute sql end:" + sql);
    }

    @Override
    public void interrupt() {
        Utils.closeQuietly(connection);
    }

    public static class Creator extends Writer.Creator {
        @JsonProperty
        public String mysql;
        //    @JsonProperty
//    public String sql;
        @JsonProperty
        public String table;
        @JsonProperty
        public String column;
        @JsonProperty
        public String pre;
        @JsonProperty
        public String post;

        private boolean valid() {
            return table != null && column != null &&
                    table.matches("^[a-zA-Z0-9_]+$") &&
                    column.matches("^[a-zA-Z0-9_,]+$");
        }

        @Override
        public List<MysqlWriter> create() throws JobParamErrorException {
            if (!valid()) {
                throw new JobParamErrorException();
            }
            String sql = String.format("load data local infile 'tmpfile' into table %s (%s)",
                    table, column);
            return ImmutableList.of(new MysqlWriter(mysql, sql, pre, post));
        }
    }
}
