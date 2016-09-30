package com.sf.stinift.plugin;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.sf.stinift.exchange.Pushable;
import com.sf.stinift.plugin.test.TestReader;
import com.sf.stinift.utils.Infoable;
import com.sf.stinift.utils.JsonInheritable;
import com.sf.stinift.worker.WorkerContext;

import java.util.List;

public abstract class Reader implements Infoable {
    private String name;

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Start up this reader.
     * <p/>
     * It will keep pushing data info {@code pushable} until {@link #interrupt()} is called,
     * an exception accour, or push job finish and return result(true or false).
     * <p/>
     * Node: Remember to call {@link Pushable#flush()} before return, the system won't do it for you!
     * And {@link Pushable#close()} is handled by system, i.e. you can leave it alone.
     *
     * @param context  the runing context
     * @param pushable where reader push data info
     * @return success or not
     */
    public abstract boolean start(WorkerContext context, Pushable pushable) throws Exception;

    /**
     * Interrupt the reader before it finish.
     */
    public abstract void interrupt();

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TestReader.Creator.class)
    public static abstract class Creator extends JsonInheritable.Class {

        public abstract List<? extends Reader> create() throws JobParamErrorException;

    }
}
