package com.vipxf.regulation.ilv.core;

import org.springframework.util.StringUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Context {
    private String zipFilePath;

    private String workDir;

    private Map<String, String> vars = new HashMap<>();

    public Context(String dir, String path) {
        workDir = StringUtils.endsWithIgnoreCase(dir, File.separator)?dir.substring(0, dir.length() - 1):dir;
        zipFilePath = StringUtils.startsWithIgnoreCase(path, File.separator)?path.substring(1):path;
    }

    public String get(String varName) {
        return vars.get(varName);
    }

    public String source() {
        return workDir + File.separator + zipFilePath;
    }

    public String workdir() {
        return workDir;
    }

    public void put(String key, String value) {
        vars.put(key, value);
    }

    public String resolveFile(String key) {
        for (Map.Entry<String, String> e : vars.entrySet()) {
            if (e.getKey().contains(key)) {
                return workDir + File.separator + e.getValue();
            }
        }
        return null;
    }
}
