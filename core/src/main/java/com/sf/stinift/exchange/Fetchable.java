package com.sf.stinift.exchange;

import java.util.List;

/**
 * This fetchable is NOT thread safe.
 */
public interface Fetchable {
    /**
     * Fetch one bee.
     *
     * @return a bee, if null, means dry out.
     */
    public Bee fetch();

    /**
     * Fetch multible bees.
     *
     * @param number maximum rows should takes.
     * @return the bees actually taken. If size() <= 0, means dry out.
     */
    public List<Bee> fetch(int number);
}
