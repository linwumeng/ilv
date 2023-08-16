package com.vipxf.regulation.ilv.core;

import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class Context {
    private String zipFilePath;

    private String workDir;

    private Map<String, String> vars = new HashMap<>();

    public Context(String dir, String path) {
        workDir = StringUtils.endsWithIgnoreCase(dir, "/")?dir.substring(0, dir.length() - 1):dir;
        zipFilePath = StringUtils.startsWithIgnoreCase(path, "/")?path.substring(1):path;
    }

    public String get(String varName) {
        return vars.get(varName);
    }

    public String source() {
        return workDir + "/" + zipFilePath;
    }

    public String workdir() {
        return workDir;
    }

    public void put(String key, String value) {
        vars.put(key, value);
    }
}
