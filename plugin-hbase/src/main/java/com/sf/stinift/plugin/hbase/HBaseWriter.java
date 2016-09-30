package com.sf.stinift.plugin.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Bee;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.resource.hbase.HBaseConnection;
import com.sf.stinift.worker.WorkerContext;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseWriter extends Writer {
    private static Logger log = new Logger(HBaseWriter.class);

    private static final String RowKey = "_key_";
    private static final int BatchCount = 100;

    private enum KeyConflictStrategy {
        Override("override"), // Default HBase behaviour.
        Duplicate("duplicate");

        String name;

        KeyConflictStrategy(String name) {
            this.name = name;
        }

        static KeyConflictStrategy parse(String name) {
            if (StringUtils.isEmpty(name)) {
                return Override;
            }
            for (KeyConflictStrategy strategy : KeyConflictStrategy.values()) {
                if (strategy.name.equalsIgnoreCase(name)) {
                    return strategy;
                }
            }
            return null;
        }
    }

    private static class Column {
        final byte[] family;
        final byte[] qualifier;
        final int index;

        public Column(String family, String qualifier, int index) {
            this.family = family.getBytes(Charsets.UTF_8);
            this.qualifier = qualifier.getBytes(Charsets.UTF_8);
            this.index = index;
        }
    }

    private final String resourceName;
    private final String tableName;
    private int rowKeyIndex;
    private List<Column> columns;
    private KeyConflictStrategy strategy;

    private volatile boolean interrupted = false;

    public HBaseWriter(String hbase, String table, int rowKeyIndex, List<Column> columns, KeyConflictStrategy strategy) {
        this.resourceName = hbase;
        this.tableName = table;
        this.rowKeyIndex = rowKeyIndex;
        this.columns = columns;
        this.strategy = strategy;
    }

    @Override
    public String info() {
        return String.format("hbase resource: %s", resourceName);
    }

    @Override
    public boolean start(WorkerContext context, Fetchable fetchable) throws Exception {
        HBaseConnection hBaseResource = (HBaseConnection) context.getResource(resourceName);
        switch (strategy) {
            case Override:
                return overrideWrite(hBaseResource.connection(), fetchable);
            case Duplicate:
                return duplicateWrite(hBaseResource.connection(), fetchable);
        }
        return false;
    }

    private Put toPut(Row row, boolean addSalt) {
        String rowKey = row.getField(rowKeyIndex);
        if (StringUtils.isEmpty(rowKey)) {
            rowKey = "";
            log.warn("[%s]: rowkey is empty", row);
        }
        if (addSalt) {
            rowKey = rowKey + "_" + System.currentTimeMillis() + "_" + RandomStringUtils.randomAlphanumeric(5);
        }
        Put put = new Put(rowKey.getBytes(Charsets.UTF_8));
        for (Column column : columns) {
            String value = row.getField(column.index);
            if (value != null) {
                put.addColumn(column.family, column.qualifier, value.getBytes(Charsets.UTF_8));
            }
        }
        return put;
    }

    private Get toGet(Row row) {
        String rowKey = row.getField(rowKeyIndex);
        if (StringUtils.isEmpty(rowKey)) {
            rowKey = "";
            log.warn("[%s]: rowkey is empty", row);
        }
        return new Get(rowKey.getBytes(Charsets.UTF_8));
    }

    private boolean overrideWrite(Connection connection, Fetchable fetchable) throws IOException {
        BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(tableName));
        try {
            Row row;
            while ((row = (Row) fetchable.fetch()) != null && !interrupted) {
                mutator.mutate(toPut(row, false));
            }
        } finally {
            IOUtils.closeQuietly(mutator);
        }
        return true;
    }

    private boolean duplicateWrite(Connection connection, Fetchable fetchable) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(tableName));
        try {
            List<Get> checks = Lists.newArrayListWithCapacity(BatchCount);
            List<Put> puts = Lists.newArrayListWithCapacity(BatchCount);
            List<Bee> bees;
            while ((bees = fetchable.fetch(BatchCount)).size() > 0 && !interrupted) {
                for (Bee bee : bees) {
                    checks.add(toGet((Row) bee));
                }
                // Check wheter those keys exists or not.
                boolean[] exists = table.existsAll(checks);
                for (int index = 0; index < bees.size(); index++) {
                    puts.add(toPut((Row) bees.get(index), exists[index]));
                }
                mutator.mutate(puts);

                checks.clear();
                puts.clear();
            }
        } finally {
            IOUtils.closeQuietly(table);
            IOUtils.closeQuietly(mutator);
        }
        return true;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    public static class Creator extends Writer.Creator {
        @JsonProperty
        public String hbase;
        @JsonProperty
        public String table;
        @JsonProperty
        public String columns;
        @JsonProperty
        public String strategy;

        @Override
        public List<HBaseWriter> create() {
            String[] ss = columns.split(",");
            List<Column> colList = new ArrayList<>();
            int rowKeyIndex = -1;
            for (int index = 0; index < ss.length; index++) {
                String s = ss[index].trim();
                if (RowKey.equals(s)) {
                    rowKeyIndex = index;
                } else {
                    String[] cf_q = s.split(":");
                    if (cf_q.length != 2) {
                        throw new RuntimeException(String.format("illegal column: %s", s));
                    }
                    colList.add(new Column(cf_q[0].trim(), cf_q[1].trim(), index));
                }
            }
            if (rowKeyIndex == -1) {
                throw new RuntimeException(String.format("row key column not set: %s", columns));
            }
            KeyConflictStrategy strategyEnum = KeyConflictStrategy.parse(strategy);
            if (strategyEnum == null) {
                throw new RuntimeException(String.format("illegal key conflict strategy: %s", strategy));
            }
            return ImmutableList.of(new HBaseWriter(hbase, table, rowKeyIndex, colList, strategyEnum));
        }
    }
}
