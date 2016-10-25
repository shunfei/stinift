package com.sf.stinift.utils;

import com.sf.stinift.Stinift;
import com.sf.stinift.config.Config;
import com.sf.stinift.config.JobConfigParser;
import com.sf.stinift.log.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class StiniftMain {
    private static final Logger log = new Logger(StiniftMain.class);

    private String filename;
    private Map<String, String> prop;

    private Map<String, String> loadprop(String[] proplist) {
        Map<String, String> result = new HashMap<String, String>();
        if (proplist != null) {
            for (int i = 0; i < proplist.length; i++) {
                int index = proplist[i].indexOf('=');
                if (index != -1) {
                    String key = proplist[i].substring(0, index);
                    String value = proplist[i].substring(index + 1);
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                        result.put(key, value);
                    }
                }
            }
        }
        return result;
    }

    private void parseArgs(String[] args) throws ParseException {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("f", "file", true, "job config path");
        options.addOption("h", "help", false, "print the usage help");
        options.addOption(null, "var", true, "job params");
        CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("stinift", options, true);
            System.exit(0);
        }
        if (!commandLine.hasOption("f")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("stinift", "require a jobfile path", options, null, true);
            System.exit(0);
        }

        if (commandLine.hasOption("var")) {
            prop = loadprop(commandLine.getOptionValues("var"));
        } else {
            prop = new HashMap<String, String>();
        }

        filename = commandLine.getOptionValue("f");
    }

    private void run() throws Exception {
        Config.init();
        final Stinift stinift = new Stinift(JobConfigParser.parseConfig(filename, prop));
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

    public static void main(String[] args) throws Exception {

        StiniftMain stiniftMain = new StiniftMain();
        stiniftMain.parseArgs(args);

        stiniftMain.run();
    }
}
