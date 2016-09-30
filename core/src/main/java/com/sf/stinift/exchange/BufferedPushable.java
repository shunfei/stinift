package com.sf.stinift.exchange;

import com.sf.stinift.utils.BoundedFifoQueue;

import java.util.Queue;

public class BufferedPushable implements Pushable {
    private final Pushable agent;
    private final int bufferSize;
    private final Queue<Bee> buffer;

    public BufferedPushable(Pushable agent, int bufferSize) {
        this.agent = agent;
        this.bufferSize = bufferSize;
        // We don't use ArrayBlockingQueue as no need to be threadsafe here.
        this.buffer = new BoundedFifoQueue<Bee>(bufferSize);
    }

    @Override
    public boolean push(Bee bee) {
        if (buffer.size() >= bufferSize) {
            if (!flush()) {
                return false;
            }
        }
        buffer.add(bee);
        return true;
    }

    @Override
    public boolean push(Iterable<Bee> bees) {
        for (Bee row : bees) {
            boolean suc = push(row);
            if (!suc) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean flush() {
        boolean suc = agent.push(buffer);
        buffer.clear();
        return suc && agent.flush();
    }

    @Override
    public void close() {
        agent.close();
    }
}
