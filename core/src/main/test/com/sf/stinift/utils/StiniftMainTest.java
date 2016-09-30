package com.sf.stinift.utils;

import com.sf.stinift.Stinift;
import com.sf.stinift.config.Config;
import com.sf.stinift.config.JobConfigParser;

import org.junit.Test;

/**
 * Created by scut_DELL on 15/11/5.
 */
public class StiniftMainTest {

    @Test
    public void testMain() throws Exception {
        Config.init();
        final Stinift stinift = new Stinift(JobConfigParser.parseConfig("/Users/scut_DELL/project/stinift/example/hive_write.json"));
        Runtime.getRuntime().addShutdownHook(
                new Thread("Shutdown hook") {
                    @Override
                    public void run() {
                        System.out.println("System shutting down...");
                        stinift.interrupt();
                    }
                }
        );
        stinift.start();
    }

}
