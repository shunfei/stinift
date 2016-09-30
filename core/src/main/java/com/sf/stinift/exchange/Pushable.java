package com.sf.stinift.exchange;

/**
 * This fetchable is NOT thread safe.
 */
public interface Pushable {
    /**
     * Push one bee.
     *
     * @param bee the one to push
     * @return success or not
     */
    public boolean push(Bee bee);

    /**
     * Push mutiple bees.
     * <p/>
     * Node that the order of bees should be ensoured by pushers. Rows will be handled like
     * <pre>
     *   for (Row row : bees) {
     *     // Handle logics, like flush row to database.
     *   }
     * </pre>
     *
     * @param bees those will be pushed
     * @return sucess or not
     */
    public boolean push(Iterable<Bee> bees);

    /**
     * Flush the caching stuff.
     * If using a buffered implementation, caching rows should be clear to under level.
     */
    public boolean flush();

    /**
     * Close this source, means no more data push info.
     * Any later push operation will become illegal, i.e. return false.
     * No side effect being called mutiple times.
     * <p/>
     * A {@link Pushable} MUST be close after finish pushing data, otherwise the system will keep wating, forever!
     */
    public void close();

}
