package com.sf.stinift.utils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.util.List;

import javax.annotation.Nullable;

public class Utils {

    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            // Do nothing
        }
    }

    public static List<String> wildcardFiles(String path) {
        File file = new File(path);
        String dirName = file.getParent();
        if (dirName == null) {
            dirName = ".";
        }
        String fileName = file.getName();

        File dir = new File(dirName);
        FileFilter fileFilter = new WildcardFileFilter(fileName);
        File[] files = dir.listFiles(fileFilter);
        if (files == null) {
            throw new RuntimeException(String.format("[%s] is not a valid path", path));
        }
        return Lists.transform(
                Lists.newArrayList(files), new Function<File, String>() {
                    @Nullable
                    @Override
                    public String apply(File file) {
                        return file.getAbsolutePath();
                    }
                }
        );
    }
}
