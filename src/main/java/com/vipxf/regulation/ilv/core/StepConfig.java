package com.vipxf.regulation.ilv.core;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class StepConfig {
    private String step;
    private List<String> source;
    private String target;
    private Map<String, Object> config;
}
