package com.sf.stinift.config;

import com.sf.stinift.JobParam;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by scut_DELL on 15/11/5.
 */
public class JobConfigParserTest {

    @Before
    public void init() {
        Config.init();
    }

    @Test
    public void testParseConfig() {
        try {
            JobParam jobParam = JobConfigParser.parseConfig("example/hbase.json");
            System.out.println(jobParam.resources.toString());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
    }

    @Test
    public void testHashMap() {

    }

}
