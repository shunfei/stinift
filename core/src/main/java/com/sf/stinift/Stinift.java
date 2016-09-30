package com.sf.stinift;

import com.sf.stinift.config.Config;
import com.sf.stinift.log.Logger;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.ResourcesBase;
import com.sf.stinift.worker.Worker;
import com.sf.stinift.worker.WorkerContext;

import org.apache.commons.io.IOUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Stinift {
    private static final Logger log = new Logger(Stinift.class);

    private final List<Worker> workers;
    private final Map<String, Resource> resources;
    private final long interruptThreadDelay;
    private final long killThreadDelay;
    private final ExecutorService stopThreads = Executors.newSingleThreadExecutor();
    private final AtomicBoolean hasInterrupted = new AtomicBoolean(false);

    public Stinift(JobParam param) {
        this.workers = param.workers;
        this.resources = param.resources;
        this.interruptThreadDelay = Config.systemConfig.interruptThreadDelay();
        this.killThreadDelay = Config.systemConfig.killThreadDelay();

        Worker.Callback callback = new Worker.Callback() {
            @Override
            public void onFinish(final Worker worker) {
                if (worker.curStatus() == Worker.Status.Fail) {
                    if (!stopThreads.isShutdown()) {
                        stopThreads.execute(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        interrupt();
                                    }
                                }
                        );
                    }
                }
            }
        };
        for (Worker worker : workers) {
            worker.addCallback(callback);
        }
    }

    /**
     * Start this stinift, block until all jobs are done, face exceptions, or {@link #interrupt()} is called.
     */
    public void start() throws Exception {
        try {
            for (Resource resource : resources.values()) {
                resource.open();
            }
            for (Worker worker : workers) {
                WorkerContext context = new WorkerContext(
                        ResourcesBase.create(resources),
                        new Thread(worker, worker.name())
                );
                worker.setContext(context);
            }
            for (Worker worker : workers) {
                worker.getContext().workerThread().start();
            }
            for (Worker worker : workers) {
                worker.getContext().workerThread().join();
            }
            hasInterrupted.set(true);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            for (Resource resource : resources.values()) {
                IOUtils.closeQuietly(resource);
            }
            stopThreads.shutdown();
        }
    }

    /**
     * Stop this stinift, block until finish. Return imediately if already stopped.
     *
     * @return always true currently
     */
    public boolean interrupt() {
        // Only call once.
        if (!hasInterrupted.compareAndSet(false, true)) {
            return true;
        }
        if (!isRunning()) {
            return true;
        }

        log.info("interrupt workers");
        for (Worker worker : workers) {
            if (worker.getContext().workerThread().isAlive()) {
                worker.interrupt();
            }
        }

        long startMS = System.currentTimeMillis();
        boolean threadInterrupted = false;
        while (isRunning()) {
            long duration = System.currentTimeMillis() - startMS;
            if (duration >= interruptThreadDelay && !threadInterrupted) {
                log.info("interrupt threads");
                for (Worker worker : workers) {
                    Thread thread = worker.getContext().workerThread();
                    if (thread.isAlive()) {
                        thread.interrupt();
                    }
                }
                threadInterrupted = true;
            } else if (duration >= killThreadDelay) {
                log.info("kill threads");
                for (Worker worker : workers) {
                    Thread thread = worker.getContext().workerThread();
                    if (thread.isAlive()) {
                        // Lets be tough!
                        thread.stop();
                    }
                }
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        return true;
    }

    public boolean isRunning() {
        for (Worker worker : workers) {
            if (worker.getContext().workerThread().isAlive()) {
                return true;
            }
        }
        return false;
    }
}
