package com.sf.stinift.plugin.test;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Pushable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.worker.WorkerContext;

import java.util.List;

public class TestReader extends Reader {
    private final int rowCount;
    private final int columnNum;
    private final int failCount;

    private WorkerContext context;

    public TestReader(int rowCount, int columnNum, int failCount) {
        this.rowCount = rowCount;
        this.columnNum = columnNum;
        this.failCount = failCount;
    }

    @Override
    public String info() {
        return String.format("rowCount: %s, columnNum: %s", rowCount, columnNum);
    }

    @Override
    public boolean start(WorkerContext context, Pushable pushable) throws Exception {
        context = context;
        for (int i = 0; i < rowCount && !context.workerThread().isInterrupted(); i++) {
            Row row = new Row(columnNum);
            for (int ci = 0; ci < columnNum; ci++) {
                row.setField(ci, String.format("msg_%s_%s", i, ci));
            }
            if (!pushable.push(row)) {
                return false;
            }
            pushable.flush();
            if (i == failCount) {
                throw new RuntimeException("Wahla!");
            }
        }
        return pushable.flush();
    }

    @Override
    public void interrupt() {
        if (context != null) {
            context.workerThread().interrupt();
        }
    }

    public static class Creator extends Reader.Creator {
        @JsonProperty
        public Integer rowCount;
        @JsonProperty
        public Integer columnNum;
        @JsonProperty
        public Integer failCount;

        @Override
        public List<? extends Reader> create() {
            rowCount = (rowCount == null) ? 100 : rowCount;
            columnNum = (columnNum == null) ? 4 : columnNum;
            failCount = (failCount == null) ? -1 : failCount;
            return ImmutableList.of(new TestReader(rowCount, columnNum, failCount));
        }
    }
}
