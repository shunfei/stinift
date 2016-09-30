package com.sf.stinift.plugin;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Bee;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.worker.WorkerContext;

import java.util.List;

public class CountingWriter extends Writer {
    private final int infoStep;
    private volatile boolean interrupted = false;

    public CountingWriter(int infoStep) {
        this.infoStep = infoStep;
    }

    @Override
    public String info() {
        return String.format("infoStep: %d", infoStep);
    }

    @Override
    public boolean start(WorkerContext context, Fetchable fetchable) throws Exception {
        int count = 0;
        while (!interrupted && !context.workerThread().isInterrupted()) {
            Bee bee = fetchable.fetch();
            if (bee == null) {
                break;
            }
            count++;
            if (count % infoStep == 0) {
                System.out.println(String.format("received %d", count));
            }
        }
        System.out.println(String.format("received %d", count));
        return true;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    public static class Creator extends Writer.Creator {
        @JsonProperty
        public Integer infoStep;

        @Override
        public List<? extends Writer> create() {
            infoStep = (infoStep == null) ? 1000 : infoStep;
            return ImmutableList.of(new CountingWriter(infoStep));
        }
    }
}
