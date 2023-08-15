package com.vipxf.regulation.ilv.core;

import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

@Data
public class StepConfig {
    private String step;
    private String source;
    private String target;
    private Map<String, Object> config;

    public String getSourceVarName() {
        return String.join(".", step, "source", StringUtils.startsWithIgnoreCase(source, "$")?source.substring(1):source);
    }
}
