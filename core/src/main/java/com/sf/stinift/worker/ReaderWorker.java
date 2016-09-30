package com.sf.stinift.worker;

import com.sf.stinift.exchange.Pushable;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.Reader;

public class ReaderWorker extends Worker {
    private static final Logger log = new Logger(ReaderWorker.class);

    private final Reader reader;
    private final Pushable pushable;

    public ReaderWorker(Reader reader, Pushable pushable) {
        super(String.format("Reader %s", reader.name()));
        this.reader = reader;
        this.pushable = pushable;
    }

    @Override
    public boolean doRun() throws Exception {
        try {
            return reader.start(getContext(), pushable);
        } finally {
            pushable.close();
        }
    }

    @Override
    public void doInterrupt() {
        reader.interrupt();
    }
}
