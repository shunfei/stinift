package com.sf.stinift;

import com.sf.stinift.resource.Resource;
import com.sf.stinift.worker.Worker;

import java.util.List;
import java.util.Map;

public class JobParam {
    public final Map<String, Resource> resources;
    public final List<Worker> workers;

    public JobParam(Map<String, Resource> resources, List<Worker> workers) {
        this.resources = resources;
        this.workers = workers;
    }
}
