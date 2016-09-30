package com.sf.stinift.worker;

import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.ResourcesBase;

public class WorkerContext {
    private ResourcesBase resources;
    private Thread workerThread;

    public WorkerContext(ResourcesBase resources, Thread thread) {
        this.resources = resources;
        this.workerThread = thread;
    }

    /**
     * The thread which worker code runing in.
     */
    public Thread workerThread() {
        return workerThread;
    }

    public Resource getResource(String name) {
        return resources.getResourceByName(name);
    }
}
