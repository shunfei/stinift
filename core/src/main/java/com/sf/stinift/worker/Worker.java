package com.sf.stinift.worker;

import com.google.common.collect.Lists;

import com.sf.stinift.log.Logger;

import java.util.List;

public abstract class Worker implements Runnable {
    public static enum Status {
        Init(0),
        Run(1),
        Interrupt(2),
        Success(2),
        Fail(2);

        int step;

        Status(int step) {
            this.step = step;
        }
    }

    private static final Logger log = new Logger(Worker.class);

    private final String name;
    private WorkerContext context;
    private List<Callback> callbacks = Lists.newArrayList();
    private Status curStatus = Status.Init;

    public Worker(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return name();
    }

    public Status curStatus() {
        return curStatus;
    }

    public void setContext(WorkerContext context) {
        this.context = context;
    }

    public WorkerContext getContext() {
        return context;
    }

    public void addCallback(Callback callback) {
        callbacks.add(callback);
    }

    private boolean updateStatus(Status from, Status to) {
        synchronized (this) {
            if (curStatus != from) {
                return false;
            }
            if (to.step <= curStatus.step) {
                throw new RuntimeException(
                        String.format(
                                "illegal status update, current: %s, new status: %s",
                                curStatus,
                                to
                        )
                );
            }
            curStatus = to;
            log.info("status update: %s -> %s", from, to);
        }

        if (curStatus == Status.Interrupt || curStatus == Status.Success || curStatus == Status.Fail) {
            for (Callback callback : callbacks) {
                callback.onFinish(this);
            }
        }
        return true;
    }

    @Override
    public final void run() {
        try {
            if (updateStatus(Status.Init, Status.Run)) {
                if (doRun()) {
                    updateStatus(Status.Run, Status.Success);
                } else {
                    updateStatus(Status.Run, Status.Fail);
                }
            }
        } catch (Exception e) {
            log.error(e, "worker [%s] face error", name());
            updateStatus(Status.Run, Status.Fail);
        }
    }

    public final void interrupt() {
        try {
            if (updateStatus(Status.Run, Status.Interrupt)) {
                doInterrupt();
            } else {
                updateStatus(Status.Init, Status.Interrupt);
            }
        } catch (Exception e) {
            log.error(e, "interrupt worker [%s] error", name());
        }
    }

    abstract boolean doRun() throws Exception;

    abstract void doInterrupt();

    public static interface Callback {
        public void onFinish(Worker worker);
    }
}
