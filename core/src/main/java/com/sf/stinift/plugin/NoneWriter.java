package com.sf.stinift.plugin;

import com.google.common.collect.ImmutableList;

import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.worker.WorkerContext;

import java.util.List;

public class NoneWriter extends Writer {
    volatile boolean interrupted = false;

    @Override
    public String info() {
        return "";
    }

    @Override
    public boolean start(WorkerContext context, Fetchable fetchable) throws Exception {
        while (!interrupted && !context.workerThread().isInterrupted() && fetchable.fetch() != null) {
        }
        return true;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    public static class Creator extends Writer.Creator {

        @Override
        public List<? extends Writer> create() {
            return ImmutableList.of(new NoneWriter());
        }
    }
}
