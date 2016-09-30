package com.sf.stinift.config;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.concurrent.TimeUnit;

public class SystemConfig {
    private Long interruptThreadDelay;  // ms
    private Long killThreadDelay;       // ms

    @JsonSetter
    @Inject(optional = true)
    public void setInterruptThreadDelay(@Named("stinift.sys.interruptThreadDelayMS") Long v) {
        interruptThreadDelay = v;
    }

    @JsonSetter
    @Inject(optional = true)
    public void setKillThreadDelay(@Named("stinift.sys.killThreadDelayMS") Long v) {
        killThreadDelay = v;
    }

    public long interruptThreadDelay() {
        return (interruptThreadDelay == null) ? TimeUnit.SECONDS.toMillis(5) : interruptThreadDelay;
    }

    public long killThreadDelay() {
        return (killThreadDelay == null) ? TimeUnit.SECONDS.toMillis(10) : killThreadDelay;
    }
}
