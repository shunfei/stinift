package com.sf.stinift.plugin.file;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.stinift.exchange.Pushable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.utils.Utils;
import com.sf.stinift.worker.WorkerContext;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

public class RowFileReader extends Reader {
    private final String fileName;
    private final String sep;
    private volatile boolean interrupted = false;

    public RowFileReader(String fileName, String sep) {
        this.fileName = fileName;
        this.sep = sep;
    }

    @Override
    public String info() {
        return String.format("fileName: %s, sep: %s", fileName, sep);
    }

    @Override
    public boolean start(WorkerContext context, Pushable pushable) throws Exception {
        try (BufferedReader fileReader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(fileName),
                        Charsets.UTF_8
                )
        )) {
            while (!interrupted && !context.workerThread().isInterrupted()) {
                String line = fileReader.readLine();
                if (line == null) {
                    break;
                }
                Row row;
                if (sep == null) {
                    row = new Row(1);
                    row.setField(0, line);
                } else {
                    String[] ss = line.split(sep);
                    row = new Row(ss.length);
                    for (int i = 0; i < ss.length; i++) {
                        row.setField(i, ss[i]);
                    }
                }
                pushable.push(row);
            }
            return pushable.flush();
        }
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    public static class Creator extends Reader.Creator {
        @JsonProperty
        public String file;
        @JsonProperty
        public String sep;

        @Override
        public List<? extends Reader> create() {
            return Lists.transform(
                    Utils.wildcardFiles(file), new Function<String, Reader>() {
                        @Override
                        public Reader apply(String s) {
                            return new RowFileReader(s, sep);
                        }
                    }
            );
        }
    }
}
