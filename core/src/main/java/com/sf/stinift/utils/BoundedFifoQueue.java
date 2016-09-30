package com.sf.stinift.utils;

import org.apache.commons.collections4.queue.CircularFifoQueue;

/**
 * An FIFO queue with a fixed size.
 * <p/>
 * Add oprations will fail if this queue is full.
 */
public class BoundedFifoQueue<E> extends CircularFifoQueue<E> {
    public BoundedFifoQueue(int size) {
        super(size);
    }

    @Override
    public boolean isFull() {
        return size() >= maxSize();
    }

    @Override
    public boolean add(E element) {
        return !isFull() && super.add(element);
    }
}
