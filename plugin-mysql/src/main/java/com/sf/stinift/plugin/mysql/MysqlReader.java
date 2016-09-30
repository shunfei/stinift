package com.sf.stinift.plugin.mysql;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Pushable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.JobParamErrorException;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.mysql.MysqlConnection;
import com.sf.stinift.utils.Utils;
import com.sf.stinift.worker.WorkerContext;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

public class MysqlReader extends Reader {
    private static final Logger log = new Logger(MysqlReader.class);

    private final String mysqlName;
    private final String sql;

    private Connection connection;

    public MysqlReader(String mysqlName, String sql) {
        this.mysqlName = mysqlName;
        this.sql = sql;
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
    public boolean start(WorkerContext context, Pushable pushable) throws Exception {
        Resource resource = context.getResource(mysqlName);
        if (resource == null) {
            throw new RuntimeException(String.format("mysql resource [%s] not found!", mysqlName));
        }
        MysqlConnection mysqlConn = (MysqlConnection) resource;
        try {
            connection = mysqlConn.getDataSource().getConnection();
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            int colCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Row row = new Row(colCount);
                try {
                    for (int index = 0; index < colCount; index++) {
                        row.setField(index, resultSet.getString(index + 1));
                    }
                    boolean suc = pushable.push(row);
                    if (!suc) {
                        return false;
                    }
                } catch (Exception e) {
                    log.error(e, "fail one row");
                }
            }
            return pushable.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            Utils.closeQuietly(connection);
        }
    }

    @Override
    public void interrupt() {
        Utils.closeQuietly(connection);
    }

    public static class Creator extends Reader.Creator {
        @JsonProperty
        public String mysql;
        @JsonProperty
        public String sql;
        @JsonProperty
        public String table;
        @JsonProperty
        public String column;
        @JsonProperty
        public String where;

        private boolean valid() {
            if (!StringUtils.isEmpty(sql)) {
                return sql.matches("^[^;]+$");
            } else {
                return table != null && column != null &&
                        table.matches("^[a-zA-Z0-9_]+$") &&
                        column.matches("^[a-zA-Z0-9_,]+$") &&
                        (where == null || where.matches("^[^;]$"));
            }
        }

        @Override
        public List<MysqlReader> create() throws JobParamErrorException {
            if (!valid()) {
                throw new JobParamErrorException();
            }
            if (StringUtils.isEmpty(sql)) {
                if (StringUtils.isEmpty(where)) {
                    where = "1=1";
                }
                sql = String.format("select %s from %s where (%s)",
                        column, table, where);
            }
            return ImmutableList.of(new MysqlReader(mysql, sql));
        }
    }
}
