package com.vipxf.regulation.ilv.core;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class YamlConfig {
    private Map<String, Object> env;
    private List<StepConfig> steps;

    public String workDir() {
        return (String)env.get("workDir");
    }
}
