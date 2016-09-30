package com.sf.stinift.plugin.hive;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.JobParamErrorException;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.hive.HiveConnection;
import com.sf.stinift.resource.hive.HiveSession;
import com.sf.stinift.worker.WorkerContext;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class HiveWriter extends Writer {
    private static final Logger log = new Logger(HiveWriter.class);

    private final String hiveName;
    private final String table;
    private final String partition;
    private final String column;
    private final String pre;
    private final String post;

    private HiveSession session;

    public HiveWriter(String hiveName, String table, String partition, String column, String pre, String post) {
        this.hiveName = hiveName;
        this.table = table;
        this.partition = partition;
        this.column = column;
        this.pre = pre;
        this.post = post;
    }

    @Override
    public String info() {
        return String.format(
                "hive:[%s], table[%s], partition[%s], column[%s]",
                hiveName,
                table,
                partition,
                column
        );
    }

    @Override
    public boolean start(WorkerContext context, Fetchable fetchable) throws Exception {
        Resource resource = context.getResource(hiveName);
        if (resource == null) {
            throw new RuntimeException(String.format("hive resource [%s]", hiveName));
        }
        HiveConnection hiveConn = (HiveConnection) resource;


        String tmptable = Hex.encodeHexString(UUID.randomUUID().toString().getBytes());
        String uploadFilePath = "/user/stinift/" + tmptable;

        try {
            session = hiveConn.openSession();

            if (StringUtils.isNotBlank(pre)) {
                session.execute(pre);
            }

            String path = createTable(tmptable, column, uploadFilePath);
            upload(fetchable, path);
            insertTable(table, tmptable, partition, column);

            if (StringUtils.isNotBlank(post)) {
                session.execute(pre);
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            dropTable(tmptable);
            session.close();
        }
    }

    private void dropTable(String tmpTable) throws TException {
        String dropTableSql = String.format("drop table stinift.%s", tmpTable);
        session.execute(dropTableSql);
    }

    private void insertTable(String table, String tmpTable, String partition, String column) throws TException {
        String[] queryColumns = merge(column.split(","), session.getColumns(table));
        String insertToTargetSql = String.format("insert overwrite table %s ", table);
        if (StringUtils.isNotBlank(partition)) {
            insertToTargetSql += String.format("partition(%s)\n", partition);
        } else {
            insertToTargetSql += "\n";
        }
        insertToTargetSql += String.format("select %s from stinift.%s\n", StringUtils.join(queryColumns, ","), tmpTable);
        session.execute(insertToTargetSql);
    }

    private String createTable(String table, String column, String location) throws TException {
        String[] columns = column.split(",");
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i] + " string";
        }
        String createTableSql = String.format(
                "create table stinift.%s (%s)\n", table, StringUtils.join(columns, ',')
        );
        createTableSql += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE\n";
        createTableSql += String.format("location '%s'", location);
        session.execute(createTableSql);
        return session.getTableLocation("stinift", table);
    }

    private String[] merge(String[] left, String[] right) {
        String[] result = new String[right.length];
        Set<String> allcolumns = new HashSet<String>();
        for (String column : left) {
            allcolumns.add(column);
        }
        for (int i = 0; i < right.length; i++) {
            if (allcolumns.contains(right[i])) {
                result[i] = right[i];
            } else {
                result[i] = "null";
            }
        }
        return result;
    }

    private void upload(Fetchable fetchable, String filepath) throws IOException {
        //UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("root"));
        Configuration configuration = new Configuration();
        FileSystem fs = new Path(filepath).getFileSystem(configuration);
        FSDataOutputStream outputStream = fs.create(new Path(filepath, "upload"));
        Row row;
        while ((row = (Row) fetchable.fetch()) != null) {
            outputStream.write(row.joinVals("\t").getBytes());
            outputStream.write("\n".getBytes());
        }
        outputStream.hflush();
        outputStream.close();
    }

    @Override
    public void interrupt() {
        if (session != null) {
            session.close();
        }
    }

    public static class Creator extends Writer.Creator {
        @JsonProperty
        public String hive;
        @JsonProperty
        public String table;
        @JsonProperty
        public String partition;
        @JsonProperty
        public String column;
        @JsonProperty
        public String pre;
        @JsonProperty
        public String post;

        private boolean valid() {
            return table != null && column != null &&
                    table.matches("^[a-zA-Z0-9_]+$") &&
                    column.matches("^[a-zA-Z0-9,]+$");
        }

        @Override
        public List<HiveWriter> create() throws JobParamErrorException {
            if (!valid()) {
                throw new JobParamErrorException();
            }
            return ImmutableList.of(new HiveWriter(hive, table, partition, column, pre, post));
        }
    }
}
