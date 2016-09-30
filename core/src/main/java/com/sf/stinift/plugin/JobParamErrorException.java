package com.sf.stinift.plugin;

/**
 * Created by scut_DELL on 15/11/23.
 */
public class JobParamErrorException extends Exception {

    public static final String message = "job param error!";

    public JobParamErrorException() {
        super(message);
    }

}
