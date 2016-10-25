package com.sf.stinift.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sf.stinift.config.Config;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonUtils {

    public static <T> T mapJson(String path, Class<T> clazz, Map<String, String> params) throws IOException {
        return Config.jsonMapper.readValue(readJsonFile(path, params), clazz);
    }

    public static <T> T mapJson(String path, TypeReference ref, Map<String, String> params) throws IOException {
        return Config.jsonMapper.readValue(readJsonFile(path, params), ref);
    }

    public static <T extends JsonInheritable> void fillInherit(Map<String, T> nameMap, List<T> inheritables) {
        for (T child : inheritables) {
            String parentName = child.inherit();
            if (!StringUtils.isEmpty(parentName)) {
                T parent = nameMap.get(parentName);
                if (parent == null) {
                    throw new RuntimeException(String.format("inherit parent [%s] not exists!", parentName));
                }
                JsonUtils.fillEmptyJsonProperty(parent, child);
            }
        }
    }

    public static <T extends JsonInheritable> void fillEmptyJsonProperty(T parent, T child) {
        Field[] fields = child.getClass().getFields();
        for (Field f : fields) {
            if (f.isAnnotationPresent(JsonProperty.class)) {
                try {
                    Object baseValue = f.get(parent);
                    Object value = f.get(child);
                    if (value == null) {
                        f.set(child, baseValue);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(String.format("[%s] should be public!", f.getName()), e);
                }
            }
        }
    }

    public static void fillProperties(Map<String, Object> base, Properties toFill) {
        for (Map.Entry<String, Object> e : base.entrySet()) {
            if (toFill.get(e.getKey()) == null) {
                toFill.put(e.getKey(), String.valueOf(e.getValue()));
            }
        }
    }

    private static final Pattern regex = Pattern.compile("\"@extend:.*?\"");

    public static String readJsonFile(String path, Map<String, String> params) throws IOException {
        File configFile = new File(path);
        if (!configFile.isFile() && !configFile.canRead()) {
            throw new RuntimeException(String.format("json file[%s] invalid!", path));
        }
        String dir = configFile.getParent();
        String content = IOUtils.toString(new FileInputStream(configFile));
        content = contentWithProb(content, params);

        Matcher matcher = regex.matcher(content);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String reg = matcher.group();
            String replaceFile = StringUtils.removeEnd(StringUtils.removeStart(reg, "\"@extend:"), "\"").trim();
            if (!replaceFile.startsWith("/")) {
                replaceFile = dir + "/" + replaceFile;
            }
            String replaceJson = readJsonFile(replaceFile, params);
            matcher.appendReplacement(sb, replaceJson);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static String contentWithProb(String content, Map<String, String> params) {
        if (params == null) {
            return content;
        } else {
            for (String key : params.keySet()) {
                String value = params.get(key);
                content = content.replace("${" + key + "}", value);
            }
            return content;
        }
    }
}
