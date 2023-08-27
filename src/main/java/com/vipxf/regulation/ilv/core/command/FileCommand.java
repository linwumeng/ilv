package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.Context;
import com.vipxf.regulation.ilv.core.StepConfig;

import java.util.*;

public class FileCommand implements Command {
    private StepConfig config;


    public FileCommand(StepConfig stepConfig) {
        this.config = stepConfig;
    }

    @Override
    public void execute(Context context) {
        String zipFilePath = context.source();
        String source = config.getSource(context);

        FileProcessor template = new FileProcessor(zipFilePath, source);
        template.process(config.getTarget(context), 0, 10000, (List<Map<String, Object>>) config.getConfig().get("prepare"));
        System.out.println("Processing CSV file done ");
    }
}

