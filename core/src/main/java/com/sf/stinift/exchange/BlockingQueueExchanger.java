package com.sf.stinift.exchange;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.sf.stinift.log.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingQueueExchanger implements Exchanger {
    private static final Logger log = new Logger(BlockingQueueExchanger.class);

    public static final String type = "blockingQueue";

    private final int fetchBufSize;
    private final int pushBufSize;

    private final BlockingQueue<Bee> queue;
    private AtomicInteger openPushableCount = new AtomicInteger(0);

    public BlockingQueueExchanger(int queueSize, int fetchBufSize, int pushBufSize) {
        this.fetchBufSize = fetchBufSize;
        this.pushBufSize = pushBufSize;

        queue = new ArrayBlockingQueue<Bee>(queueSize);
    }

    @Override
    public Fetchable getFetchable() {
        return new BufferedFetchable(
                new Fetchable() {
                    @Override
                    public Bee fetch() {
                        return BlockingQueueExchanger.this.fetch();
                    }

                    @Override
                    public List<Bee> fetch(int number) {
                        return BlockingQueueExchanger.this.fetch(number);
                    }
                }, fetchBufSize
        );
    }

    @Override
    public Pushable getPushable() {
        Pushable pushable = new BufferedPushable(
                new Pushable() {
                    @Override
                    public boolean push(Bee bee) {
                        return BlockingQueueExchanger.this.push(bee);
                    }

                    @Override
                    public boolean push(Iterable<Bee> bees) {
                        return BlockingQueueExchanger.this.push(bees);
                    }

                    @Override
                    public boolean flush() {
                        // Do nothing. As we cannot fore the fetchers to consume immediately.
                        return true;
                    }

                    @Override
                    public void close() {
                        openPushableCount.decrementAndGet();
                    }
                }, pushBufSize
        );
        openPushableCount.incrementAndGet();
        return pushable;
    }

    private Bee fetch() {
        try {
            while ((isOpen() || queue.size() > 0) && !Thread.currentThread().isInterrupted()) {
                Bee row = queue.poll(500, TimeUnit.MILLISECONDS);
                if (row != null) {
                    return row;
                }
            }
        } catch (InterruptedException e) {
            return null;
        }
        return null;
    }

    private List<Bee> fetch(int number) {
        if (!isOpen() && queue.size() <= 0) {
            return Collections.emptyList();
        }

        // drainTo won't wait for available data.
        // If we cannot fetch any by now, then call fetch() to wait.

        List<Bee> holder = new ArrayList<>(number);
        int count = queue.drainTo(holder, number);
        if (count > 0) {
            return holder;
        } else {
            Bee row = fetch();
            if (row == null) {
                return Collections.emptyList();
            } else {
                holder.add(row);
                return holder;
            }
        }
    }

    private boolean push(Bee bee) {
        try {
            while (isOpen() && !Thread.currentThread().isInterrupted()) {
                if (queue.offer(bee, 500, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
        } catch (InterruptedException e) {
            return false;
        }
        return false;
    }

    private boolean push(Iterable<Bee> bees) {
        for (Bee bee : bees) {
            boolean suc = push(bee);
            if (!suc) {
                return false;
            }
        }
        return true;
    }

    private boolean isOpen() {
        return openPushableCount.get() > 0;
    }

    public static class Creator implements Exchanger.Creator {
        @Inject(optional = true)
        @Named("stinift.exchanger.blockingQueue.queueSize")
        public Integer queueSize;
        @Inject(optional = true)
        @Named("stinift.exchanger.blockingQueue.fetchBufSize")
        public Integer fetchBufSize;
        @Inject(optional = true)
        @Named("stinift.exchanger.blockingQueue.pushBufSize")
        public Integer pushBufSize;

        @Override
        public Exchanger create() {
            queueSize = (queueSize == null) ? 10000 : queueSize;
            fetchBufSize = (fetchBufSize == null) ? queueSize / 4 : fetchBufSize;
            pushBufSize = (pushBufSize == null) ? queueSize / 4 : pushBufSize;

            log.debug(
                    "Creating %s, queueSize:%d, fetchBufSize:%s, pushBufSize:%s",
                    BlockingQueueExchanger.class.getSimpleName(),
                    queueSize,
                    fetchBufSize,
                    pushBufSize
            );

            return new BlockingQueueExchanger(queueSize, fetchBufSize, pushBufSize);
        }
    }
}
