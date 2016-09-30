package com.sf.stinift.plugin.test;

import com.google.common.collect.ImmutableList;

import com.sf.stinift.exchange.Bee;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.worker.WorkerContext;

import java.util.List;

public class TestWriter extends Writer {

    private WorkerContext context;

    @Override
    public String info() {
        return "test";
    }

    @Override
    public boolean start(WorkerContext context, Fetchable fetchable) throws Exception {
        this.context = context;
        Bee row = null;
        while ((row = fetchable.fetch()) != null && !context.workerThread().isInterrupted()) {
            System.out.println(row);
        }
        return true;
    }

    @Override
    public void interrupt() {
        if (context != null) {
            context.workerThread().interrupt();
        }
    }

    public static class Creator extends Writer.Creator {
        @Override
        public List<? extends Writer> create() {
            return ImmutableList.of(new TestWriter());
        }
    }
}
