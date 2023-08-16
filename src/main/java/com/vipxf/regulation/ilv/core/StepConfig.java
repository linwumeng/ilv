package com.vipxf.regulation.ilv.core;

import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.Map;

@Data
public class StepConfig {
    private String step;
    private String source;
    private String target;
    private String type;
    private Map<String, Object> config;

    public String getSourceVarName() {
        if (StringUtils.startsWithIgnoreCase(source, "$") && source.contains(".")) {
            return source;
        }
        return String.join(".", step, "source", StringUtils.startsWithIgnoreCase(source, "$")?source.substring(1):"_");
    }
    public String getTargetVarName() {
        return String.join(".", step, "target", StringUtils.startsWithIgnoreCase(source, "$")?source.substring(1):"_");
    }

    public String getSource(Context context) {
        return StringUtils.startsWithIgnoreCase(source, "$")?context.get(getSourceVarName()):source;
    }

    public String getTarget(Context context) {
        return StringUtils.startsWithIgnoreCase(target, "$")?context.workdir() + "/" + context.get(getTargetVarName()):target;
    }
}
