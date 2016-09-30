package com.sf.stinift.utils;

import com.sf.stinift.Stinift;
import com.sf.stinift.config.Config;
import com.sf.stinift.config.JobConfigParser;
import com.sf.stinift.log.Logger;

public class StiniftMain {
    private static final Logger log = new Logger(StiniftMain.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new RuntimeException("please specify job config path");
        }
        Config.init();
        final Stinift stinift = new Stinift(JobConfigParser.parseConfig(args[0]));
        Runtime.getRuntime().addShutdownHook(
                new Thread("Shutdown hook") {
                    @Override
                    public void run() {
                        log.info("System shutting down...");
                        stinift.interrupt();
                    }
                }
        );
        stinift.start();
    }
}
