package com.sf.stinift.worker;

import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.Writer;

public class WriterWorker extends Worker {
    private static final Logger log = new Logger(WriterWorker.class);

    private final Writer writer;
    private final Fetchable fetchable;

    public WriterWorker(Writer writer, Fetchable fetchable) {
        super(String.format("Writer %s", writer.name()));
        this.writer = writer;
        this.fetchable = fetchable;
    }

    @Override
    public boolean doRun() throws Exception {
        return writer.start(getContext(), fetchable);
    }

    @Override
    public void doInterrupt() {
        writer.interrupt();
    }
}
