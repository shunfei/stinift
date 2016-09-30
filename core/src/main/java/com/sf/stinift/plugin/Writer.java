package com.sf.stinift.plugin;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.utils.Infoable;
import com.sf.stinift.utils.JsonInheritable;
import com.sf.stinift.worker.WorkerContext;

import java.util.List;

public abstract class Writer implements Infoable {
    private String name;

    public final String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Start up this writer.
     * <p/>
     * It will keep fetching data from {@code readable} until {@link #interrupt()} is called,
     * an exception accour, or fetch job finish and return result(true or false).
     *
     * @param context   the runing context
     * @param fetchable where writer fetch data from
     * @return success or not
     */
    public abstract boolean start(WorkerContext context, Fetchable fetchable) throws Exception;

    /**
     * Interrupt this writer before it finish.
     */
    public abstract void interrupt();

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoneWriter.Creator.class)
    public static abstract class Creator extends JsonInheritable.Class {

        public abstract List<? extends Writer> create() throws JobParamErrorException;

    }
}
