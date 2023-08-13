package com.vipxf.regulation.ilv.core.command;

import com.vipxf.regulation.ilv.core.StepConfig;
import com.vipxf.regulation.ilv.core.command.Command;

import java.util.ArrayList;
import java.util.List;

public class CommandFactory {
    public List<Command> createCommands(List<StepConfig> stepConfigs) {
        List<Command> commands = new ArrayList<>();
        for (StepConfig stepConfig : stepConfigs) {
            Command command = createCommand(stepConfig);
            commands.add(command);
        }
        return commands;
    }

    private Command createCommand(StepConfig stepConfig) {
        // Create the appropriate command based on the stepConfig
        // Implement this according to your requirements
        switch (stepConfig.getStep()) {
            case "journal": return new JournalCommand(stepConfig);
            default: break;
        }
        return null;
    }
}
