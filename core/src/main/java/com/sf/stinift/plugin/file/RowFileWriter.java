package com.sf.stinift.plugin.file;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.plugin.Writer;
import com.sf.stinift.worker.WorkerContext;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class RowFileWriter extends Writer {
    private final String fileName;
    private final String sep;
    public final boolean append;

    private volatile boolean interrupted = false;

    public RowFileWriter(String fileName, String sep, boolean append) {
        this.fileName = fileName;
        this.sep = sep;
        this.append = append;
    }

    @Override
    public String info() {
        return String.format("fileName: %s, sep: %s", fileName, sep);
    }

    @Override
    public boolean start(WorkerContext context, Fetchable fetchable) throws Exception {
        try (OutputStreamWriter fileWriter = new OutputStreamWriter(
                new BufferedOutputStream(new FileOutputStream(fileName, append)),
                Charsets.UTF_8
        )) {
            while (!interrupted && !context.workerThread().isInterrupted()) {
                Row row = (Row) fetchable.fetch();
                if (row == null) {
                    break;
                }
                fileWriter.write(row.joinVals(sep));
                fileWriter.write("\n");
            }
            fileWriter.flush();
        }
        return true;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    public static class Creator extends Writer.Creator {
        @JsonProperty
        public String file;
        @JsonProperty
        public String sep;
        @JsonProperty
        public Boolean append;
        @JsonProperty
        public Integer split;

        @Override
        public List<? extends Writer> create() {
            sep = (sep == null) ? "," : sep;
            append = (append == null) ? true : append;
            split = (split == null) ? 0 : split;
            if (split == 0) {
                return ImmutableList.of(new RowFileWriter(file, sep, append));
            } else {
                List<RowFileWriter> writers = new ArrayList<>();
                for (int i = 0; i < split; i++) {
                    writers.add(new RowFileWriter(file + "_" + i, sep, append));
                }
                return writers;
            }
        }
    }
}
