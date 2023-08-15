package com.vipxf.regulation.ilv.core;

import java.util.HashMap;
import java.util.Map;

public class Context {
    private String zipFilePath;

    private Map<String, String> vars = new HashMap<>();

    public Context(String path) {
        zipFilePath = path;
    }

    public String get(String var) {
        return "";
    }

    public String source() {
        return zipFilePath;
    }

    public void put(String key, String value) {
        vars.put(key, value);
    }
}
