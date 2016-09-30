package com.sf.stinift.exchange;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

public interface Exchanger {
    /**
     * Obtain a {@link Fetchable} from this exchanger, where we can pull data from.
     * <p/>
     * Node that the fetchable returned should NOT be shared between workers,
     * each worker should get their own one. Because fetchable may not be threadsafe.
     *
     * @return a {@link Fetchable} used by a worker.
     */
    public Fetchable getFetchable();

    /**
     * Obtain a {@link Pushable} from this exchanger, where we can push data into.
     * <p/>
     * Node that the pushable returned should NOT be shared between workers,
     * each worker should get their own one. Because fetchable may not be threadsafe.
     *
     * @return a {@link Pushable} used by a worker.
     */
    public Pushable getPushable();

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    public static interface Creator {
        public Exchanger create();
    }
}
